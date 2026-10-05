"""Records read from a stream, and messages delivered by a consumer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Protocol

from .protocol.codec import Headers, WireRecord
from .types import Duration

__all__ = ["StreamRecord", "Message", "MessageSettler"]


class StreamRecord:
    """A record read from a stream."""

    __slots__ = ("headers", "key", "offset", "subject", "timestamp_ns", "value")

    offset: int
    #: Append time, nanoseconds since the Unix epoch.
    timestamp_ns: int
    subject: str
    #: The record's key, or ``None``.
    key: bytes | None
    value: bytes
    #: In wire order; a key can repeat. See :meth:`header`.
    headers: Headers

    def __init__(self, r: WireRecord) -> None:
        self.offset = r.offset
        self.timestamp_ns = r.timestamp_ns
        self.subject = r.subject
        self.key = r.key
        self.value = r.value
        self.headers = r.headers

    @property
    def timestamp_ms(self) -> int:
        """Append time, milliseconds since the Unix epoch."""
        return self.timestamp_ns // 1_000_000

    @property
    def timestamp(self) -> datetime:
        """Append time as an aware UTC datetime (microsecond precision)."""
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)

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

    def __repr__(self) -> str:
        return f"{type(self).__name__}(offset={self.offset}, subject={self.subject!r}, value={self.value!r:.60})"


class MessageSettler(Protocol):
    """What a message needs from its client to settle itself."""

    def ack_nowait(self, consumer: str, offsets: list[int]) -> None:
        """Queue a fire-and-forget ack."""

    async def nack(self, consumer: str, offset: int, delay: Duration | None = None) -> None:
        """Ask for redelivery."""

    async def term(self, consumer: str, offset: int, reason: str = "") -> None:
        """Dead-letter now."""

    async def in_progress(self, consumer: str, offsets: list[int]) -> None:
        """Reset ack deadlines."""


class Message(StreamRecord):
    """A record delivered by a consumer (push or pull), with the means to settle it."""

    __slots__ = ("_settler", "consumer", "delivery_count")

    #: 1 on the first delivery, incremented on each redelivery.
    delivery_count: int
    #: The consumer that delivered it.
    consumer: str

    def __init__(self, r: WireRecord, consumer: str, settler: MessageSettler) -> None:
        super().__init__(r)
        self.delivery_count = r.delivery_count
        self.consumer = consumer
        self._settler = settler

    def ack(self) -> None:
        """Acknowledge, fire-and-forget (no round trip).

        Acks made in the same event-loop iteration go out together as one
        frame. If the connection is down the ack is dropped and the record is
        redelivered. Failures the server reports surface as the client's
        ``"error"`` event. Use :meth:`ExspeedClient.ack` to wait for
        confirmation.
        """
        self._settler.ack_nowait(self.consumer, [self.offset])

    async def nack(self, delay: Duration | None = None) -> None:
        """Ask for redelivery after ``delay`` (default: the consumer's backoff)."""
        await self._settler.nack(self.consumer, self.offset, delay)

    async def term(self, reason: str = "") -> None:
        """Never redeliver: dead-letter now (to the consumer's ``dlq_stream``, if set)."""
        await self._settler.term(self.consumer, self.offset, reason)

    async def in_progress(self) -> None:
        """Still working on it: reset the ack deadline."""
        await self._settler.in_progress(self.consumer, [self.offset])
