"""The coalescing, pipelined publisher."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from .errors import ConnectionError, ProtocolError
from .protocol import messages as m
from .types import Duration, HeadersInit, PublishRecord, PublishResult, Value

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ["Publisher"]

#: Keep each batch frame well below the 16 MiB frame limit.
MAX_BATCH_BYTES = 4 * 1024 * 1024


class PublisherTransport(Protocol):
    """What a publisher needs from the client."""

    def send_request(self, req: m.Request) -> asyncio.Future[m.Response]:
        """Write a request now; the future resolves with its response."""


@dataclass
class _Queued:
    stream: str
    record: m.WirePublishRecord
    size: int
    fut: asyncio.Future[PublishResult]


class Publisher:
    """A pipelined, coalescing publisher, from :meth:`ExspeedClient.publisher`.

    Concurrent :meth:`publish` calls are gathered into ``PublishBatch``
    requests (one per run of records for the same stream), and many batches
    can be in flight at once. Records reach the stream in the order
    :meth:`publish` was called, and every call gets its own record's result.
    Use it as an async context manager to :meth:`close` it on exit.
    """

    def __init__(
        self,
        transport: PublisherTransport,
        *,
        batch_window: float = 0.0,
        max_batch_records: int = 512,
        max_in_flight: int = 4096,
    ) -> None:
        self._transport = transport
        self._batch_window = max(0.0, batch_window)
        self._max_batch_records = max(1, max_batch_records)
        self._max_in_flight = max(1, max_in_flight)
        self._queue: deque[_Queued] = deque()
        self._scheduled = False
        self._in_flight = 0
        self._permit_waiters: deque[asyncio.Future[None]] = deque()
        self._idle_waiters: list[asyncio.Future[None]] = []
        self._closed = False

    @property
    def pending(self) -> int:
        """Records accepted and not yet acknowledged."""
        return self._in_flight

    async def publish(
        self,
        stream: str,
        subject: str,
        value: Value = b"",
        *,
        key: bytes | str | None = None,
        headers: HeadersInit = None,
        msg_id: str | None = None,
        ttl: Duration | str | None = None,
        delay: Duration | str | None = None,
        deliver_at: datetime | float | None = None,
        priority: int | None = None,
    ) -> PublishResult:
        """Publish one record; returns its offset once the server has it.

        Takes the same arguments as :meth:`ExspeedClient.publish`. Waits while
        ``max_in_flight`` records are unacknowledged.
        """
        record = PublishRecord(
            subject, value, key, headers, msg_id, ttl=ttl, delay=delay, deliver_at=deliver_at, priority=priority
        )
        return await self.publish_record(stream, record)

    async def publish_record(self, stream: str, record: PublishRecord) -> PublishResult:
        """Publish a :class:`PublishRecord` (see :meth:`publish`)."""
        if self._closed:
            raise ConnectionError("publisher is closed")
        wire = record.to_wire()
        await self._acquire()
        if self._closed:
            self._release()
            raise ConnectionError("publisher is closed")
        size = (
            64
            + len(wire.subject.encode())
            + len(wire.value)
            + len(wire.key or b"")
            + len(wire.msg_id or "") * 3
            + sum(4 + len(k) * 3 + len(v) * 3 for k, v in wire.headers)
        )
        fut: asyncio.Future[PublishResult] = asyncio.get_running_loop().create_future()
        self._queue.append(_Queued(stream, wire, size, fut))
        if len(self._queue) >= self._max_batch_records:
            self._flush_queue()
        else:
            self._schedule()
        return await fut

    async def flush(self) -> None:
        """Wait until every accepted record has been acknowledged (or failed)."""
        if self._queue:
            self._flush_queue()
        if self._in_flight == 0 and not self._permit_waiters:
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._idle_waiters.append(fut)
        await fut

    async def close(self) -> None:
        """Flush, then reject further publishes."""
        await self.flush()
        self._closed = True

    async def __aenter__(self) -> Publisher:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def _acquire(self) -> None:
        if self._in_flight < self._max_in_flight and not self._permit_waiters:
            self._in_flight += 1
            return
        # FIFO hand-off keeps call order intact while waiting for capacity.
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._permit_waiters.append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self._release()  # the permit was handed over; pass it on
            else:
                with contextlib.suppress(ValueError):
                    self._permit_waiters.remove(fut)
            raise

    def _release(self) -> None:
        while self._permit_waiters:
            nxt = self._permit_waiters.popleft()
            if not nxt.done():
                nxt.set_result(None)  # the permit passes straight to the next waiter
                return
        self._in_flight -= 1
        if self._in_flight == 0:
            waiters, self._idle_waiters = self._idle_waiters, []
            for w in waiters:
                if not w.done():
                    w.set_result(None)

    def _schedule(self) -> None:
        if self._scheduled:
            return
        self._scheduled = True
        loop = asyncio.get_running_loop()

        def run() -> None:
            self._scheduled = False
            self._flush_queue()

        if self._batch_window == 0:
            loop.call_soon(run)
        else:
            loop.call_later(self._batch_window, run)

    def _flush_queue(self) -> None:
        """Send everything queued, as runs of the same stream, in arrival order."""
        q = self._queue
        while q:
            stream = q[0].stream
            run: list[_Queued] = []
            size = 0
            while (
                q
                and q[0].stream == stream
                and len(run) < self._max_batch_records
                and (not run or size + q[0].size <= MAX_BATCH_BYTES)
            ):
                item = q.popleft()
                size += item.size
                run.append(item)
            self._send_run(stream, run)

    def _send_run(self, stream: str, run: list[_Queued]) -> None:
        """The request is written synchronously, so wire order matches arrival order."""
        single = len(run) == 1
        req: m.Request = m.Publish(stream, run[0].record) if single else m.PublishBatch(stream, [q.record for q in run])

        def settle(results: list[PublishResult] | None, err: BaseException | None) -> None:
            for i, q in enumerate(run):
                if not q.fut.done():
                    if results is not None:
                        q.fut.set_result(results[i])
                    else:
                        q.fut.set_exception(err or ConnectionError("publish failed"))
            for _ in run:
                self._release()

        try:
            sent = self._transport.send_request(req)
        except Exception as e:
            settle(None, e)
            return

        def done(f: asyncio.Future[m.Response]) -> None:
            if f.cancelled():
                settle(None, ConnectionError("publish cancelled"))
                return
            exc = f.exception()
            if exc is not None:
                settle(None, exc)
                return
            resp = f.result()
            if single and isinstance(resp, m.PublishOk):
                settle([PublishResult(resp.offset, resp.duplicate)], None)
            elif isinstance(resp, m.PublishBatchOk) and len(resp.results) == len(run):
                settle([PublishResult(o, d) for o, d in resp.results], None)
            else:
                settle(None, ProtocolError(f"unexpected reply to {type(req).__name__}: {type(resp).__name__}"))

        sent.add_done_callback(done)
