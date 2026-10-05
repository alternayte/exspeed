"""Push subscriptions to consumers."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from ._util import wait_future
from .connection import Connection, SubscriptionSink
from .message import Message, MessageSettler
from .protocol import messages as m
from .protocol.codec import WireRecord

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ["EndReason", "Subscription"]


@dataclass(frozen=True)
class EndReason:
    """Why a subscription ended.

    ``code`` is 404 when the consumer or its stream was deleted, 503 when the
    node lost leadership or the connection was lost and not re-established,
    0 when it ended locally (``unsubscribe()`` or ``client.close()``). Other
    codes come from a failed re-subscribe after a reconnect.
    """

    code: int
    message: str


class SubscriptionHost(MessageSettler, Protocol):
    """What a subscription needs from its client."""

    def forget(self, sub: Subscription) -> None:
        """The subscription ended."""


class Subscription(SubscriptionSink):
    """A push subscription to a consumer, from :meth:`ExspeedClient.subscribe`.

    Iterate it with ``async for``; iteration ends when the server ends the
    subscription (see :attr:`end_reason`), on :meth:`unsubscribe`, or when the
    client closes. Use it as an async context manager to unsubscribe on exit::

        async with client.subscribe("billing") as sub:
            async for msg in sub:
                ...
                msg.ack()

    Credit flow: the server pushes at most ``window`` records ahead of your
    code. Each record you take returns one credit (sent in batches of half
    the window), so a slow consumer slows delivery instead of buffering
    without bound.
    """

    def __init__(self, host: SubscriptionHost, consumer: str, window: int) -> None:
        #: The consumer's name.
        self.consumer = consumer
        #: The credit window.
        self.window = window
        self._host = host
        self._conn: Connection | None = None
        self._sub_id = 0
        self._buffer: deque[WireRecord] = deque()
        self._consumed = 0
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._end_reason: EndReason | None = None

    @property
    def id(self) -> int:
        """Server-assigned id of the current subscription (changes after a reconnect)."""
        return self._sub_id

    @property
    def end_reason(self) -> EndReason | None:
        """Why the subscription ended, or ``None`` while it is active."""
        return self._end_reason

    @property
    def closed(self) -> bool:
        """True once the subscription has ended."""
        return self._end_reason is not None

    @property
    def buffered(self) -> int:
        """Records received but not yet taken."""
        return len(self._buffer)

    async def next(self, timeout: float | None = None) -> Message | None:
        """The next message, or ``None`` once the subscription has ended or,
        with ``timeout`` (seconds), when nothing arrived in time."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if self._buffer:
                return self._take()
            if self._end_reason is not None:
                return None
            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append(fut)
            try:
                left = None if deadline is None else deadline - loop.time()
                if not await wait_future(fut, left):
                    return self._take() if self._buffer else None
            finally:
                if not fut.done():
                    fut.cancel()
                with contextlib.suppress(ValueError):
                    self._waiters.remove(fut)

    def __aiter__(self) -> Subscription:
        return self

    async def __anext__(self) -> Message:
        msg = await self.next()
        if msg is None:
            raise StopAsyncIteration
        return msg

    async def __aenter__(self) -> Subscription:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.unsubscribe()

    async def unsubscribe(self) -> None:
        """Stop delivery.

        Records delivered to this subscription and not yet acked (including
        buffered ones your code never saw) are redelivered by the server.
        """
        if self._end_reason is not None:
            return
        conn, sub_id = self._conn, self._sub_id
        self.end(EndReason(0, "unsubscribed"), keep_buffered=False)
        if conn is not None and not conn.closed:
            conn.remove_sub(sub_id)
            with contextlib.suppress(Exception):
                await conn.request(m.Unsubscribe(sub_id))

    #: Alias of :meth:`unsubscribe`.
    aclose = unsubscribe

    # ---- SubscriptionSink (called by the connection) ------------------------

    def on_subscribed(self, conn: Connection, sub_id: int) -> None:
        """Internal: the (re-)subscribe succeeded."""
        if self._end_reason is not None:
            # Unsubscribed while a (re-)subscribe was in flight.
            conn.remove_sub(sub_id)
            conn.send(m.Unsubscribe(sub_id))
            return
        self._conn = conn
        self._sub_id = sub_id
        self._consumed = 0

    def on_deliver(self, records: list[WireRecord]) -> None:
        """Internal: records pushed by the server."""
        if self._end_reason is not None:
            return
        self._buffer.extend(records)
        self._wake(len(records))

    def on_ended(self, code: int, message: str) -> None:
        """Internal: server-side end; buffered records are still yielded."""
        self.end(EndReason(code, message), keep_buffered=True)

    # ---- client hooks ---------------------------------------------------------

    def suspend(self) -> None:
        """Internal: the connection was lost; a re-subscribe follows. Buffered
        records are dropped (the server redelivers them)."""
        self._conn = None
        self._buffer.clear()
        self._consumed = 0

    def end(self, reason: EndReason, keep_buffered: bool) -> None:
        """Internal: end the subscription."""
        if self._end_reason is not None:
            return
        self._end_reason = reason
        self._conn = None
        if not keep_buffered:
            self._buffer.clear()
        self._host.forget(self)
        self._wake(len(self._waiters))

    def _wake(self, n: int) -> None:
        while n > 0 and self._waiters:
            w = self._waiters.popleft()
            if not w.done():
                w.set_result(None)
                n -= 1

    def _take(self) -> Message:
        r = self._buffer.popleft()
        self._return_credit()
        return Message(r, self.consumer, self._host)

    def _return_credit(self) -> None:
        conn = self._conn
        if conn is None or conn.closed:
            return
        self._consumed += 1
        if self._consumed >= max(1, self.window // 2):
            credits, self._consumed = self._consumed, 0
            conn.send(m.Credit(self._sub_id, credits))

    def __repr__(self) -> str:
        return f"Subscription(consumer={self.consumer!r}, id={self._sub_id}, end_reason={self._end_reason!r})"
