"""Core (non-persistent) messaging: messages and subscriptions."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from typing import TYPE_CHECKING, Any, Protocol

from ._util import wait_future
from .connection import Connection, SubscriptionSink
from .errors import ExspeedError
from .protocol import messages as m
from .protocol.codec import Headers
from .subscription import EndReason
from .types import HeadersInit, Value

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ["CoreMessage", "CoreSubscription"]


class CoreHost(Protocol):
    """What core messages and subscriptions need from the client."""

    async def publish_core(
        self, subject: str, value: Value = b"", *, headers: HeadersInit = None, reply_to: str | None = None
    ) -> None:
        """Publish a core message."""

    def forget_core(self, sub: CoreSubscription) -> None:
        """The subscription ended."""


class CoreMessage:
    """A core message: from a core subscription, or the response to a request."""

    __slots__ = ("_host", "headers", "reply_to", "subject", "value")

    subject: str
    #: Set on a request: answer it with :meth:`respond`.
    reply_to: str | None
    #: In wire order; a key can repeat. See :meth:`header`.
    headers: Headers
    value: bytes

    def __init__(self, msg: m.CoreMsg, host: CoreHost) -> None:
        self.subject = msg.subject
        self.reply_to = msg.reply_to
        self.headers = msg.headers
        self.value = msg.value
        self._host = host

    def text(self) -> str:
        """The value as UTF-8 text."""
        return self.value.decode("utf-8")

    def json(self) -> Any:
        """The value parsed as JSON."""
        return json.loads(self.value)

    def header(self, name: str) -> str | None:
        """The first header named ``name``, or ``None``."""
        for k, v in self.headers:
            if k == name:
                return v
        return None

    async def respond(self, value: Value, *, headers: HeadersInit = None) -> None:
        """Answer a request: publish ``value`` to its ``reply_to`` subject."""
        if not self.reply_to:
            raise ExspeedError("message has no reply_to")
        await self._host.publish_core(self.reply_to, value, headers=headers)

    def __repr__(self) -> str:
        return f"CoreMessage(subject={self.subject!r}, reply_to={self.reply_to!r}, value={self.value!r:.60})"


class CoreSubscription(SubscriptionSink):
    """A core-message subscription, from :meth:`ExspeedClient.subscribe_core`.

    Iterate it with ``async for``; iteration ends on :meth:`unsubscribe`, when
    the server ends it (503 when leadership moves), or when the client
    closes. Use it as an async context manager to unsubscribe on exit.

    Core messages are not stored: a subscription receives what is published
    while it is live. After a reconnect the client subscribes again with the
    same subject and queue group; messages published in the gap are missed.
    """

    def __init__(self, host: CoreHost, subject: str, queue: str | None) -> None:
        #: The subject filter.
        self.subject = subject
        #: The queue group, or ``None``.
        self.queue = queue
        self._host = host
        self._conn: Connection | None = None
        self._sub_id = 0
        self._buffer: deque[CoreMessage] = deque()
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._end_reason: EndReason | None = None

    @property
    def id(self) -> int:
        """Server-assigned id of the current subscription (high bit set; changes after a reconnect)."""
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
        """Messages received but not yet taken."""
        return len(self._buffer)

    async def next(self, timeout: float | None = None) -> CoreMessage | None:
        """The next message, or ``None`` once the subscription has ended or,
        with ``timeout`` (seconds), when nothing arrived in time."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if self._buffer:
                return self._buffer.popleft()
            if self._end_reason is not None:
                return None
            fut: asyncio.Future[None] = loop.create_future()
            self._waiters.append(fut)
            try:
                left = None if deadline is None else deadline - loop.time()
                if not await wait_future(fut, left):
                    return self._buffer.popleft() if self._buffer else None
            finally:
                if not fut.done():
                    fut.cancel()
                with contextlib.suppress(ValueError):
                    self._waiters.remove(fut)

    def __aiter__(self) -> CoreSubscription:
        return self

    async def __anext__(self) -> CoreMessage:
        msg = await self.next()
        if msg is None:
            raise StopAsyncIteration
        return msg

    async def __aenter__(self) -> CoreSubscription:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.unsubscribe()

    async def unsubscribe(self) -> None:
        """Stop delivery. Buffered messages are dropped."""
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

    # ---- SubscriptionSink -------------------------------------------------------

    def on_subscribed(self, conn: Connection, sub_id: int) -> None:
        """Internal: the (re-)subscribe succeeded."""
        if self._end_reason is not None:
            conn.remove_sub(sub_id)
            conn.send(m.Unsubscribe(sub_id))
            return
        self._conn = conn
        self._sub_id = sub_id

    def on_core_msg(self, msg: m.CoreMsg) -> None:
        """Internal: a message pushed by the server."""
        if self._end_reason is not None:
            return
        self._buffer.append(CoreMessage(msg, self._host))
        self._wake(1)

    def on_ended(self, code: int, message: str) -> None:
        """Internal: server-side end; buffered messages are still yielded."""
        self.end(EndReason(code, message), keep_buffered=True)

    # ---- client hooks -----------------------------------------------------------

    def suspend(self) -> None:
        """Internal: the connection was lost; a re-subscribe follows. Buffered messages are kept."""
        self._conn = None

    def end(self, reason: EndReason, keep_buffered: bool) -> None:
        """Internal: end the subscription."""
        if self._end_reason is not None:
            return
        self._end_reason = reason
        self._conn = None
        if not keep_buffered:
            self._buffer.clear()
        self._host.forget_core(self)
        self._wake(len(self._waiters))

    def _wake(self, n: int) -> None:
        while n > 0 and self._waiters:
            w = self._waiters.popleft()
            if not w.done():
                w.set_result(None)
                n -= 1

    def __repr__(self) -> str:
        return f"CoreSubscription(subject={self.subject!r}, queue={self.queue!r}, id={self._sub_id})"
