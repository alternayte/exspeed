"""Key-value buckets."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, Protocol

from ._util import wait_future
from .errors import ProtocolError, ServerError
from .protocol import messages as m
from .protocol.codec import WireRecord
from .protocol.constants import ErrorCode
from .types import Duration, Value, duration_ms, encode_value, parse_json

if TYPE_CHECKING:
    from .client import ReadResult

__all__ = ["KV_OP_HEADER", "KvOp", "KvEntry", "KvBucket", "KvWatch"]

#: Header marking a tombstone: ``DEL`` or ``PURGE``.
KV_OP_HEADER = "exspeed-kv-op"

#: What a revision holds.
KvOp = Literal["put", "delete", "purge"]


class _KvRecord(Protocol):
    offset: int
    timestamp_ns: int
    subject: str
    value: bytes
    headers: list[tuple[str, str]]


@dataclass(frozen=True)
class KvEntry:
    """One revision of a key."""

    key: str
    value: bytes
    #: The record's offset in the bucket's stream plus one (0 = absent).
    revision: int
    #: Write time, ns since the Unix epoch.
    timestamp_ns: int
    #: ``"delete"`` and ``"purge"`` entries are tombstones with an empty value.
    op: KvOp

    @classmethod
    def from_record(cls, r: _KvRecord) -> KvEntry:
        """Build from a record of the bucket's stream (offset + 1 = revision)."""
        op_header = next((v for k, v in r.headers if k == KV_OP_HEADER), None)
        op: KvOp = "delete" if op_header == "DEL" else "purge" if op_header == "PURGE" else "put"
        return cls(r.subject, r.value, r.offset + 1, r.timestamp_ns, op)

    @property
    def timestamp_ms(self) -> int:
        """Write time, ms since the Unix epoch."""
        return self.timestamp_ns // 1_000_000

    @property
    def timestamp(self) -> datetime:
        """Write time as an aware UTC datetime."""
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

    def text(self) -> str:
        """The value as UTF-8 text."""
        return self.value.decode("utf-8")

    def json(self) -> Any:
        """The value parsed as JSON."""
        return json.loads(self.value)


class KvHost(Protocol):
    """What a bucket needs from the client."""

    async def raw_request(self, req: m.Request, *, timeout: float | None = None) -> m.Response:
        """Send a request on the current connection."""

    async def read(
        self,
        stream: str,
        *,
        from_offset: int = 0,
        max_records: int = 100,
        max_bytes: int = 0,
        wait: Duration = 0,
        filter: str = "",
    ) -> ReadResult:
        """Stateless read."""

    async def delete_stream(self, name: str) -> None:
        """Delete a stream."""


def _expect(req: m.Request, resp: m.Response, cls: type[Any]) -> Any:
    if not isinstance(resp, cls):
        raise ProtocolError(f"unexpected reply to {type(req).__name__}: {type(resp).__name__}")
    return resp


class KvBucket:
    """A key-value bucket, from :meth:`ExspeedClient.kv`.

    The bucket ``B`` is the stream ``KV_B``: each key is a subject, each put a
    record, and a key's revision is its record's offset plus one.
    """

    def __init__(self, host: KvHost, bucket: str) -> None:
        #: The bucket's name.
        self.bucket = bucket
        self._host = host

    @property
    def stream(self) -> str:
        """The bucket's stream (``KV_<bucket>``)."""
        return f"KV_{self.bucket}"

    async def create(self, *, history: int = 0, ttl: Duration | None = None, max_bytes: int = 0) -> None:
        """Create the bucket. Idempotent for the same settings.

        Args:
            history: Values kept per key, 1 to 64 (0 = 1).
            ttl: Keys expire this long after their last put (``None`` = never).
            max_bytes: Size limit in bytes (0 = the server's default).
        """
        req = m.KvCreateBucket(
            self.bucket,
            history=history,
            ttl_ms=0 if ttl is None else duration_ms(ttl, "ttl", 1),
            max_bytes=max_bytes,
        )
        _expect(req, await self._host.raw_request(req), m.Ok)

    async def destroy(self) -> None:
        """Delete the bucket and every key in it."""
        await self._host.delete_stream(self.stream)

    async def get(self, key: str) -> KvEntry | None:
        """The current value of ``key``, or ``None`` when it is absent, deleted or expired.

        Raises :class:`ServerError` 404 when the bucket doesn't exist.
        """
        return await self._get(key, None)

    async def get_revision(self, key: str, revision: int) -> KvEntry | None:
        """``key`` at ``revision``, while the bucket still keeps it; else ``None``."""
        return await self._get(key, revision)

    async def _get(self, key: str, revision: int | None) -> KvEntry | None:
        req = m.KvGet(self.bucket, key, revision)
        try:
            resp = await self._host.raw_request(req)
        except ServerError as e:
            # 404 is "key '...' not found" or "bucket '...' not found".
            if e.code == ErrorCode.NOT_FOUND and e.message.startswith("key "):
                return None
            raise
        records: list[WireRecord] = _expect(req, resp, m.Messages).records
        return KvEntry.from_record(records[0]) if records else None

    async def put(
        self, key: str, value: Value, *, ttl: Duration | None = None, expected_revision: int | None = None
    ) -> int:
        """Set ``key``; returns the new revision.

        Args:
            ttl: Expire this key this long after the put.
            expected_revision: Only if the key is at this revision (0 = absent);
                else :class:`ServerError` 409 with ``detail["current_revision"]``.
        """
        return await self._write(
            m.KvPut(
                self.bucket,
                key,
                encode_value(value),
                expected_revision=expected_revision,
                ttl_ms=None if ttl is None else duration_ms(ttl, "ttl", 1),
            )
        )

    async def create_key(self, key: str, value: Value, *, ttl: Duration | None = None) -> int:
        """Set ``key`` only if it doesn't exist (or was deleted); :class:`ServerError` 409 otherwise."""
        return await self.put(key, value, ttl=ttl, expected_revision=0)

    async def update(self, key: str, value: Value, revision: int, *, ttl: Duration | None = None) -> int:
        """Set ``key`` only if it is at ``revision`` (compare-and-set); :class:`ServerError` 409 otherwise."""
        return await self.put(key, value, ttl=ttl, expected_revision=revision)

    async def delete(self, key: str, *, expected_revision: int | None = None) -> int:
        """Delete ``key`` (its history stays until it ages out); returns the tombstone's revision."""
        return await self._write(m.KvDelete(self.bucket, key, False, expected_revision))

    async def purge(self, key: str, *, expected_revision: int | None = None) -> int:
        """Delete ``key`` and hide its older values; returns the tombstone's revision."""
        return await self._write(m.KvDelete(self.bucket, key, True, expected_revision))

    async def _write(self, req: m.Request) -> int:
        resp: m.PublishOk = _expect(req, await self._host.raw_request(req), m.PublishOk)
        return resp.offset

    async def keys(self, filter: str = "") -> list[str]:
        """Keys that have a value, matching ``filter`` (NATS-style; ``""`` = all), sorted."""
        req = m.KvKeys(self.bucket, filter)
        resp: m.Json = _expect(req, await self._host.raw_request(req), m.Json)
        keys = parse_json(resp.data, "KvKeys")
        if not isinstance(keys, list):
            raise ProtocolError("bad JSON in reply to KvKeys: not an array")
        return [str(k) for k in keys]

    async def history(self, key: str) -> list[KvEntry]:
        """Kept revisions of ``key``, oldest first (deletes included)."""
        req = m.KvHistory(self.bucket, key)
        resp: m.Messages = _expect(req, await self._host.raw_request(req), m.Messages)
        return [KvEntry.from_record(r) for r in resp.records]

    def watch(self, filter: str = "") -> KvWatch:
        """Watch keys matching ``filter`` (``""`` = all): first the current value
        of every matching key (deleted keys left out), then every change as it
        happens, deletes included."""
        return KvWatch(self._host, self.stream, filter)

    def __repr__(self) -> str:
        return f"KvBucket({self.bucket!r})"


#: Long-poll wait of each follow-up read (seconds).
WATCH_WAIT = 10.0
WATCH_BATCH = 1000


class KvWatch:
    """Changes to a bucket's keys, from :meth:`KvBucket.watch`.

    Iterate it with ``async for``, or call :meth:`next`. It reads the
    bucket's stream with stateless long-poll reads, so it holds no state on
    the server. A failed read (for example :class:`ConnectionError` when the
    connection drops) is raised from :meth:`next`. Use it as an async context
    manager to stop it on exit.
    """

    def __init__(self, host: KvHost, stream: str, filter: str) -> None:
        self._host = host
        self._stream = stream
        self._filter = filter
        self._from = 0
        self._snapshot_done = False
        self._pending: deque[KvEntry] = deque()
        self._fetching: asyncio.Task[None] | None = None
        self._stopped = False

    @property
    def closed(self) -> bool:
        """True after :meth:`stop`."""
        return self._stopped

    async def next(self, timeout: float | None = None) -> KvEntry | None:
        """The next entry, waiting for changes as long as it takes; with
        ``timeout`` (seconds), ``None`` when nothing arrived in time. ``None``
        after :meth:`stop`."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if self._stopped:
                return None
            if self._pending:
                return self._pending.popleft()
            task = self._fill()
            left = None if deadline is None else deadline - loop.time()
            if not await wait_future(task, left):
                return None
            if self._fetching is task:
                self._fetching = None
            exc = None if task.cancelled() else task.exception()
            if exc is not None:
                raise exc

    def stop(self) -> None:
        """Stop watching: :meth:`next` returns ``None`` from now on."""
        self._stopped = True
        self._pending.clear()
        task, self._fetching = self._fetching, None
        if task is not None and not task.done():
            task.cancel()

    def __aiter__(self) -> KvWatch:
        return self

    async def __anext__(self) -> KvEntry:
        e = await self.next()
        if e is None:
            raise StopAsyncIteration
        return e

    async def __aenter__(self) -> KvWatch:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.stop()

    async def aclose(self) -> None:
        """Stop watching (see :meth:`stop`)."""
        self.stop()

    def _fill(self) -> asyncio.Task[None]:
        """One read at a time; a ``next()`` that timed out leaves it running, and its records are kept."""
        task = self._fetching
        if task is None or task.done():
            coro = self._follow() if self._snapshot_done else self._load_snapshot()
            task = asyncio.get_running_loop().create_task(coro)
            task.add_done_callback(_retrieve)
            self._fetching = task
        return task

    async def _load_snapshot(self) -> None:
        """Read up to the high watermark seen by the first read, keep the last
        record per key, and queue the live keys sorted by revision."""
        latest: dict[str, KvEntry] = {}
        end: int | None = None
        while True:
            r = await self._host.read(
                self._stream, from_offset=self._from, max_records=WATCH_BATCH, filter=self._filter
            )
            if end is None:
                end = r.high_watermark
            for rec in r.records:
                e = KvEntry.from_record(rec)
                latest[e.key] = e
            progressed = r.next_offset > self._from
            self._from = max(self._from, r.next_offset)
            if self._from >= end or not progressed:
                break
        if self._stopped:
            return
        live = sorted((e for e in latest.values() if e.op == "put"), key=lambda e: e.revision)
        self._pending.extend(live)
        self._snapshot_done = True

    async def _follow(self) -> None:
        r = await self._host.read(
            self._stream,
            from_offset=self._from,
            max_records=WATCH_BATCH,
            wait=WATCH_WAIT,
            filter=self._filter,
        )
        self._from = max(self._from, r.next_offset)
        if self._stopped:
            return
        self._pending.extend(KvEntry.from_record(rec) for rec in r.records)


def _retrieve(task: asyncio.Task[None]) -> None:
    # Mark the exception as retrieved; next() re-raises it to its caller.
    if not task.cancelled():
        with contextlib.suppress(BaseException):
            task.exception()
