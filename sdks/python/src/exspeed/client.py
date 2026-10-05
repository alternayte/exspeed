"""The Exspeed client."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import random
import secrets
import ssl
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from ._util import AwaitableContext, wait_future
from .connection import Connection, ConnectionOptions, SubscriptionSink
from .core import CoreMessage, CoreSubscription
from .errors import ConnectionError, ExspeedError, ProtocolError, ServerError, TimeoutError
from .kv import KvBucket
from .message import Message, StreamRecord
from .protocol import messages as m
from .protocol.constants import DEFAULT_PORT, ErrorCode, SeekKind
from .publisher import Publisher
from .subscription import EndReason, Subscription
from .types import (
    ConsumerInfo,
    ConsumerSpec,
    Duration,
    HeadersInit,
    Metadata,
    PublishRecord,
    PublishResult,
    QueryResult,
    ReconnectOptions,
    SeekTarget,
    SeekTime,
    ServerInfo,
    StreamInfo,
    StreamSpec,
    TlsOptions,
    Value,
    duration_ms,
    duration_seconds,
    encode_value,
    epoch_ms,
    parse_json,
    to_headers,
)

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ["ExspeedClient", "ReadResult", "Event", "connect"]

log = logging.getLogger("exspeed")

R = TypeVar("R", bound=m.Response)

#: Client events: ``"disconnect"`` ``(err)``, ``"reconnect"`` ``(ServerInfo)``,
#: ``"close"`` ``(err or None)``, ``"error"`` ``(err)``.
Event = Literal["disconnect", "reconnect", "close", "error"]

#: Subjects of request-reply inboxes start with this token.
INBOX_PREFIX = "_INBOX"


@dataclass
class ReadResult:
    """Result of a stateless :meth:`ExspeedClient.read`."""

    records: list[StreamRecord]
    #: Pass as ``from_offset`` to continue.
    next_offset: int
    #: The stream's next offset at the time of the read.
    high_watermark: int


@dataclass
class _Inbox:
    prefix: str
    conn: Connection | None = None
    sub_id: int = 0
    ready: asyncio.Task[None] | None = field(default=None, repr=False)


class _InboxSink(SubscriptionSink):
    def __init__(self, client: ExspeedClient, inbox: _Inbox) -> None:
        self._client = client
        self._inbox = inbox

    def on_subscribed(self, conn: Connection, sub_id: int) -> None:
        if self._client._inbox is not self._inbox:
            # Replaced (connection lost) while subscribing: release it.
            conn.remove_sub(sub_id)
            conn.send(m.Unsubscribe(sub_id))
            return
        self._inbox.conn = conn
        self._inbox.sub_id = sub_id

    def on_core_msg(self, msg: m.CoreMsg) -> None:
        self._client._on_reply(msg)

    def on_ended(self, code: int, message: str) -> None:
        if self._client._inbox is self._inbox:
            self._client._drop_inbox(ServerError(code, message))


class _Host:
    """What subscriptions, messages and core messages call back into."""

    def __init__(self, client: ExspeedClient) -> None:
        self._c = client

    def forget(self, sub: Subscription) -> None:
        self._c._subs.discard(sub)

    def forget_core(self, sub: CoreSubscription) -> None:
        self._c._core_subs.discard(sub)

    def ack_nowait(self, consumer: str, offsets: list[int]) -> None:
        self._c.ack_nowait(consumer, offsets)

    async def nack(self, consumer: str, offset: int, delay: Duration | None = None) -> None:
        await self._c.nack(consumer, offset, delay)

    async def term(self, consumer: str, offset: int, reason: str = "") -> None:
        await self._c.term(consumer, offset, reason)

    async def in_progress(self, consumer: str, offsets: list[int]) -> None:
        await self._c.in_progress(consumer, offsets)

    async def publish_core(
        self, subject: str, value: Value = b"", *, headers: HeadersInit = None, reply_to: str | None = None
    ) -> None:
        await self._c.publish_core(subject, value, headers=headers, reply_to=reply_to)


def _ssl_context(
    tls: bool | ssl.SSLContext | TlsOptions | None,
) -> tuple[ssl.SSLContext | None, str | None]:
    if tls is None or tls is False:
        return None, None
    if tls is True:
        return ssl.create_default_context(), None
    if isinstance(tls, ssl.SSLContext):
        return tls, None
    if isinstance(tls, TlsOptions):
        return tls.context(), tls.server_hostname
    raise ExspeedError(f"invalid tls option: {tls!r}")


class ExspeedClient:
    """A connection to an Exspeed server (client protocol v2).

    One client is one TCP/TLS connection. Requests are multiplexed by
    correlation id, so a slow pull or long-poll read never blocks other
    calls; share one client across your application. Create one with
    :meth:`connect` (or :func:`exspeed.connect`)::

        async with exspeed.connect("127.0.0.1", 5933) as client:
            await client.publish("orders", "orders.placed", {"id": 1})

    Events (see :meth:`on`): ``"disconnect"`` (the connection dropped;
    reconnecting), ``"reconnect"`` (reconnected; subscriptions restored),
    ``"close"`` (closed for good) and ``"error"`` (a fire-and-forget request
    such as an ack failed).
    """

    def __init__(
        self,
        conn: Connection,
        opts: ConnectionOptions,
        reconnect: ReconnectOptions | None,
        servers: list[str],
    ) -> None:
        """Internal; use :meth:`connect`."""
        self._conn = conn
        self._opts = opts
        self._reconnect_opts = reconnect
        self._servers = servers
        self._state: Literal["connected", "reconnecting", "closed"] = "connected"
        self._subs: set[Subscription] = set()
        self._core_subs: set[CoreSubscription] = set()
        self._host = _Host(self)
        self._inbox: _Inbox | None = None
        self._replies: dict[str, asyncio.Future[CoreMessage]] = {}
        self._next_reply = 1
        self._ephemeral: dict[str, ConsumerSpec] = {}
        self._pending_acks: dict[str, list[int]] = {}
        self._ack_flush_scheduled = False
        self._listeners: dict[str, list[Callable[..., Any]]] = {}
        self._reconnect_task: asyncio.Task[None] | None = None
        self._background: set[asyncio.Task[Any]] = set()

    # ---- connecting -------------------------------------------------------------

    @classmethod
    def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = DEFAULT_PORT,
        *,
        servers: Iterable[str] | None = None,
        token: str | None = None,
        tls: bool | ssl.SSLContext | TlsOptions | None = None,
        client_id: str = "exspeed-py",
        request_timeout: float = 30.0,
        keepalive: float = 20.0,
        reconnect: bool | ReconnectOptions = True,
        verify_crc: bool = False,
    ) -> AwaitableContext[ExspeedClient]:
        """Connect and authenticate. Await it, or use it with ``async with``
        (which closes the client on exit). The first connection attempt is
        not retried.

        Args:
            host: Server host.
            port: Server port.
            servers: Cluster seed addresses (``"host:port"``). The client
                connects to whichever node leads, following leader hints, and
                finds the new leader after a failover. Overrides host/port.
            token: Bearer token, when the server runs with auth.
            tls: ``True`` for TLS with the system CAs, a :class:`TlsOptions`
                (custom CA, client certificate for mutual TLS), or a ready
                :class:`ssl.SSLContext`.
            client_id: Client name sent in the handshake and shown in server logs.
            request_timeout: Seconds to wait for a response (on top of a pull's
                or read's own wait). Also bounds connecting.
            keepalive: Ping interval in seconds; the server drops connections
                idle for 120 s. 0 disables.
            reconnect: Reconnect automatically when the connection drops
                (default), ``False``, or :class:`ReconnectOptions`.
            verify_crc: Check every received record's CRC32C (pure Python, so
                it costs CPU on large reads).
        """
        ctx, server_hostname = _ssl_context(tls)
        opts = ConnectionOptions(
            host=host,
            port=port,
            client_id=client_id,
            token=token,
            ssl_context=ctx,
            server_hostname=server_hostname,
            request_timeout=request_timeout,
            keepalive=keepalive,
            verify_crc=verify_crc,
        )
        if reconnect is True:
            rc: ReconnectOptions | None = ReconnectOptions()
        elif reconnect is False:
            rc = None
        else:
            rc = reconnect
        return AwaitableContext(cls._connect(opts, rc, list(servers or [])), ExspeedClient.close)

    @classmethod
    async def _connect(
        cls, opts: ConnectionOptions, reconnect: ReconnectOptions | None, servers: list[str]
    ) -> ExspeedClient:
        holder: list[ExspeedClient] = []

        def on_close(conn: Connection, err: Exception) -> None:
            if holder:
                holder[0]._on_connection_lost(conn, err)

        def on_async_error(err: ExspeedError) -> None:
            if holder:
                holder[0]._emit("error", err)

        conn = await _open_leader(opts, servers, None, on_close, on_async_error)
        client = cls(conn, opts, reconnect, servers)
        holder.append(client)
        if conn.closed:  # lost between the handshake and now
            client._on_connection_lost(conn, ConnectionError("connection closed"))
        return client

    @property
    def server_info(self) -> ServerInfo:
        """Handshake info from the current connection."""
        return self._conn.info

    @property
    def connected(self) -> bool:
        """True while a connection is up (False while reconnecting or after :meth:`close`)."""
        return self._state == "connected" and not self._conn.closed

    async def close(self) -> None:
        """Close the connection.

        Pending requests fail with :class:`ConnectionError`, subscriptions end
        (code 0), and the server deletes this connection's ephemeral consumers.
        """
        if self._state == "closed":
            return
        self._flush_acks()  # acks made just before close() still go out
        self._state = "closed"
        task, self._reconnect_task = self._reconnect_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        for sub in list(self._subs):
            sub.end(EndReason(0, "client closed"), keep_buffered=False)
        for core in list(self._core_subs):
            core.end(EndReason(0, "client closed"), keep_buffered=False)
        self._drop_inbox(ConnectionError("client closed"))
        await self._conn.close()
        self._emit("close", None)

    async def __aenter__(self) -> ExspeedClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # ---- events -----------------------------------------------------------------

    def on(self, event: Event, callback: Callable[..., Any]) -> None:
        """Call ``callback`` on ``event``. Coroutine functions are run as tasks.

        - ``"disconnect"`` ``(err)``: the connection dropped; reconnecting.
        - ``"reconnect"`` ``(info: ServerInfo)``: reconnected; subscriptions restored.
        - ``"close"`` ``(err | None)``: closed for good (by :meth:`close`, or
          because reconnecting gave up).
        - ``"error"`` ``(err: ServerError | ProtocolError)``: a fire-and-forget
          request (ack, credit) failed.
        """
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: Event, callback: Callable[..., Any]) -> None:
        """Stop calling ``callback`` on ``event``."""
        with contextlib.suppress(ValueError, KeyError):
            self._listeners[event].remove(callback)

    def _emit(self, event: str, *args: Any) -> None:
        listeners = list(self._listeners.get(event, ()))
        if not listeners and event == "error":
            log.debug("exspeed async error (no 'error' listener): %s", args[0] if args else "")
        for cb in listeners:
            try:
                r = cb(*args)
                if inspect.isawaitable(r):
                    self._spawn(r)
            except Exception:
                log.exception("exspeed %r listener failed", event)

    def _spawn(self, aw: Any) -> None:
        async def run() -> None:
            await aw

        task = asyncio.get_running_loop().create_task(run())
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # ---- basics -----------------------------------------------------------------

    async def ping(self) -> float:
        """Round trip to the server; returns the latency in seconds."""
        start = time.perf_counter()
        await self._call(m.Ping(), m.Pong)
        return time.perf_counter() - start

    async def metadata(self) -> Metadata:
        """Node id, leadership and server version."""
        d = await self._json(m.Metadata())
        return Metadata(
            node_id=str(d.get("node_id", "")),
            is_leader=bool(d.get("is_leader", False)),
            leader=d.get("leader") if isinstance(d.get("leader"), str) else None,
            server_version=str(d.get("server_version", "")),
        )

    # ---- streams ----------------------------------------------------------------

    async def create_stream(self, spec: StreamSpec | str) -> None:
        """Create a stream. Idempotent when it exists with the same settings;
        :class:`ServerError` 409 when the settings differ."""
        s = StreamSpec(spec) if isinstance(spec, str) else spec
        await self._call(m.CreateStream(s.to_wire()), m.Ok)

    async def update_stream(self, spec: StreamSpec) -> None:
        """Replace a stream's settings (unset fields reset to the server defaults)."""
        await self._call(m.UpdateStream(spec.to_wire()), m.Ok)

    async def delete_stream(self, name: str) -> None:
        """Delete a stream. Fails with 409 while consumers exist (``detail["consumers"]``)."""
        await self._call(m.DeleteStream(name), m.Ok)

    async def stream_info(self, name: str) -> StreamInfo:
        """A stream's bounds and settings."""
        return StreamInfo.from_json(await self._json(m.StreamInfo(name)))

    async def list_streams(self) -> list[StreamInfo]:
        """The streams this credential can see."""
        items = await self._json_list(m.ListStreams())
        return [StreamInfo.from_json(x) for x in items if isinstance(x, dict)]

    # ---- publishing -------------------------------------------------------------

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
        """Publish one record and wait for its offset.

        Args:
            stream: The stream.
            subject: The record's subject (``orders.placed``).
            value: Bytes are sent as-is, ``str`` as UTF-8, anything else as JSON.
            key: Partition/compaction key.
            headers: A mapping or ``(key, value)`` pairs.
            msg_id: Idempotency key (see :func:`new_msg_id`).
            ttl: Expire the record this long after the append (seconds, a
                timedelta, or ``"30s"``-style strings). Needs ``allow_msg_ttl``.
            delay: Deliver to consumers no earlier than this long after the
                append. Needs ``allow_delayed``.
            deliver_at: Deliver to consumers no earlier than this time (a
                datetime or a Unix timestamp in seconds). Needs ``allow_delayed``.
            priority: 0 (default) to 9, for consumers with a ``priority_window``.
        """
        record = PublishRecord(
            subject, value, key, headers, msg_id, ttl=ttl, delay=delay, deliver_at=deliver_at, priority=priority
        )
        return await self.publish_record(stream, record)

    async def publish_record(self, stream: str, record: PublishRecord) -> PublishResult:
        """Publish a :class:`PublishRecord` and wait for its offset."""
        r = await self._call(m.Publish(stream, record.to_wire()), m.PublishOk)
        return PublishResult(r.offset, r.duplicate)

    async def publish_batch(self, stream: str, records: Iterable[PublishRecord]) -> list[PublishResult]:
        """Publish several records in one request; one result per record, in order."""
        wire = [r.to_wire() for r in records]
        if not wire:
            return []
        r = await self._call(m.PublishBatch(stream, wire), m.PublishBatchOk)
        return [PublishResult(o, d) for o, d in r.results]

    def publisher(
        self, *, batch_window: float = 0.0, max_batch_records: int = 512, max_in_flight: int = 4096
    ) -> Publisher:
        """A coalescing, order-preserving publisher on this client (see :class:`Publisher`).

        Args:
            batch_window: Seconds to gather records before sending a batch; 0
                sends whatever was published in the same event-loop iteration.
            max_batch_records: Most records per batch request.
            max_in_flight: Records accepted but not yet acknowledged; publish waits beyond this.
        """
        return Publisher(
            self, batch_window=batch_window, max_batch_records=max_batch_records, max_in_flight=max_in_flight
        )

    # ---- stateless reads --------------------------------------------------------

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
        """Read records without a consumer. Continue from ``next_offset``.

        Args:
            from_offset: First offset to read.
            max_records: Most records (the server caps it at 10,000).
            max_bytes: Byte budget; 0 = the server default (1 MiB).
            wait: Long-poll: when caught up, wait up to this long for new records.
            filter: NATS-style subject filter (``orders.*``, ``orders.>``); ``""`` = all.
        """
        wait_ms = duration_ms(wait, "wait")
        r = await self._call(
            m.Read(stream, from_offset, max_records, max_bytes, wait_ms, filter),
            m.ReadResult,
            timeout=self._opts.request_timeout + wait_ms / 1000,
        )
        return ReadResult([StreamRecord(w) for w in r.records], r.next_offset, r.high_watermark)

    # ---- SQL ----------------------------------------------------------------------

    async def query(self, sql: str) -> QueryResult:
        """Run a bounded ExQL query. Needs a global-admin credential when auth is on."""
        d = await self._json(m.Query(sql))
        return QueryResult(
            columns=[str(c) for c in d.get("columns", [])],
            rows=list(d.get("rows", [])),
            row_count=int(d.get("row_count", 0)),
            execution_time_ms=int(d.get("execution_time_ms", 0)),
            truncated=bool(d.get("truncated", False)),
        )

    # ---- consumers ----------------------------------------------------------------

    async def create_consumer(self, spec: ConsumerSpec) -> ConsumerInfo:
        """Create a consumer. Idempotent for an identical spec; 409 when one of
        that name exists with a different spec."""
        info = ConsumerInfo.from_json(await self._json(m.CreateConsumer(spec.to_wire())))
        if spec.ephemeral:
            self._ephemeral[spec.name] = spec
        return info

    async def delete_consumer(self, name: str) -> None:
        """Delete a consumer."""
        await self._call(m.DeleteConsumer(name), m.Ok)
        self._ephemeral.pop(name, None)

    async def consumer_info(self, name: str) -> ConsumerInfo:
        """A consumer's settings, position and counters."""
        return ConsumerInfo.from_json(await self._json(m.ConsumerInfo(name)))

    async def list_consumers(self, stream: str | None = None) -> list[ConsumerInfo]:
        """Consumers this credential can see, optionally only those on ``stream``."""
        items = await self._json_list(m.ListConsumers(stream))
        return [ConsumerInfo.from_json(x) for x in items if isinstance(x, dict)]

    async def seek(self, consumer: str, to: SeekTarget) -> None:
        """Move a consumer's cursor: ``"earliest"``, ``"latest"``, an offset
        (``int``), a :class:`datetime.datetime`, or a :class:`SeekTime`."""
        if to == "earliest":
            kind, value = SeekKind.EARLIEST, 0
        elif to == "latest":
            kind, value = SeekKind.LATEST, 0
        elif isinstance(to, int) and not isinstance(to, bool):
            kind, value = SeekKind.OFFSET, to
        elif isinstance(to, datetime):
            kind, value = SeekKind.TIME, epoch_ms(to, "seek time")
        elif isinstance(to, SeekTime):
            kind, value = SeekKind.TIME, to.time_ms
        else:
            raise ExspeedError(f"invalid seek target: {to!r}")
        await self._call(m.SeekConsumer(consumer, kind, value), m.Ok)

    def subscribe(self, consumer: str, *, window: int = 256) -> AwaitableContext[Subscription]:
        """Start push delivery from a consumer. Await it, or use it with
        ``async with`` (which unsubscribes on exit).

        Any number of subscriptions (on any connection, in any process) can
        share one consumer; each record goes to one of them.

        Args:
            window: Credit window: how many records the server may push before
                your code has taken them. Credit is returned as you iterate.
        """
        return AwaitableContext(self._subscribe(consumer, window), Subscription.unsubscribe)

    async def _subscribe(self, consumer: str, window: int) -> Subscription:
        window = max(1, min(int(window), 0xFFFFFFFF))
        sub = Subscription(self._host, consumer, window)
        # The connection binds `sub` to its id as soon as SubscribeOk arrives,
        # before any Deliver behind it is routed.
        await self._call(m.Subscribe(consumer, window), m.SubscribeOk, sink=sub)
        if not sub.closed:
            self._subs.add(sub)
        return sub

    async def pull(
        self, consumer: str, *, max_messages: int = 100, max_bytes: int = 0, expires: Duration = 5.0
    ) -> list[Message]:
        """Fetch up to ``max_messages``, waiting up to ``expires`` for at least
        one. Returns an empty list on timeout."""
        expires_ms = duration_ms(expires, "expires")
        r = await self._call(
            m.Pull(consumer, max_messages, max_bytes, expires_ms),
            m.Messages,
            timeout=self._opts.request_timeout + expires_ms / 1000,
        )
        return [Message(w, consumer, self._host) for w in r.records]

    async def ack(self, consumer: str, offsets: Iterable[int]) -> None:
        """Acknowledge records and wait for the server to confirm."""
        await self._call(m.Ack(consumer, list(offsets)), m.Ok)

    def ack_nowait(self, consumer: str, offsets: Iterable[int]) -> None:
        """Queue a fire-and-forget ack (what :meth:`Message.ack` does).

        Acks queued in the same event-loop iteration go out as one ``Ack``
        frame per consumer, before any later request.
        """
        if self._state != "connected":
            return  # redelivered after the reconnect
        self._pending_acks.setdefault(consumer, []).extend(offsets)
        if not self._ack_flush_scheduled:
            self._ack_flush_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush_acks)

    async def nack(self, consumer: str, offset: int, delay: Duration | None = None) -> None:
        """Redeliver after ``delay`` (default: the consumer's backoff)."""
        delay_ms = 0 if delay is None else min(duration_ms(delay, "delay"), 0xFFFFFFFF)
        await self._call(m.Nack(consumer, offset, delay_ms), m.Ok)

    async def term(self, consumer: str, offset: int, reason: str = "") -> None:
        """Dead-letter now (to the consumer's ``dlq_stream``, if set)."""
        await self._call(m.Term(consumer, offset, reason), m.Ok)

    async def in_progress(self, consumer: str, offsets: Iterable[int]) -> None:
        """Reset the ack deadlines of records still being worked on."""
        await self._call(m.InProgress(consumer, list(offsets)), m.Ok)

    # ---- core messaging -----------------------------------------------------------

    async def publish_core(
        self, subject: str, value: Value = b"", *, headers: HeadersInit = None, reply_to: str | None = None
    ) -> None:
        """Publish a core message to the core subscriptions live now.

        Nothing is stored and delivery is at most once. Returns once the server
        accepted it. With ``reply_to`` it fails with :class:`ServerError` 404
        when nobody received it (:meth:`request` sets this for you).
        """
        await self._call(m.CorePublish(subject, reply_to, to_headers(headers), encode_value(value)), m.Ok)

    def subscribe_core(self, subject: str, *, queue: str | None = None) -> AwaitableContext[CoreSubscription]:
        """Receive core messages on subjects matching ``subject`` (a filter such
        as ``orders.*``). With a ``queue`` group, each message goes to one
        member of the group. Await it, or use it with ``async with``."""
        return AwaitableContext(self._subscribe_core(subject, queue), CoreSubscription.unsubscribe)

    async def _subscribe_core(self, subject: str, queue: str | None) -> CoreSubscription:
        sub = CoreSubscription(self._host, subject, queue)
        await self._call(m.CoreSubscribe(subject, queue), m.SubscribeOk, sink=sub)
        if not sub.closed:
            self._core_subs.add(sub)
        return sub

    async def request(
        self, subject: str, value: Value = b"", *, headers: HeadersInit = None, timeout: float | None = None
    ) -> CoreMessage:
        """Send a request (a core message with a reply subject) and return the first response.

        Fails at once with :class:`ServerError` 404 when nobody is subscribed
        to ``subject``, and with :class:`TimeoutError` after ``timeout``
        seconds (default: the request timeout). All requests on a connection
        share one inbox subscription (``_INBOX.<random>.*``), set up by the
        first request.
        """
        t = self._opts.request_timeout if timeout is None else duration_seconds(timeout, "timeout")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + t
        payload = encode_value(value)
        hdrs = to_headers(headers)
        try:
            prefix = await asyncio.wait_for(self._ensure_inbox(), t)
        except asyncio.TimeoutError:
            raise TimeoutError(f"request to {subject} timed out after {t:g} s") from None
        token = str(self._next_reply)
        self._next_reply += 1
        fut: asyncio.Future[CoreMessage] = loop.create_future()
        self._replies[token] = fut
        try:
            left = max(deadline - loop.time(), 0.001)
            await self._call(m.CorePublish(subject, f"{prefix}.{token}", hdrs, payload), m.Ok, timeout=left)
            if not await wait_future(fut, deadline - loop.time()):
                raise TimeoutError(f"request to {subject} timed out after {t:g} s")
            return fut.result()
        finally:
            if self._replies.get(token) is fut:
                del self._replies[token]
            if not fut.done():
                fut.cancel()
            elif not fut.cancelled():
                fut.exception()  # mark retrieved

    async def _ensure_inbox(self) -> str:
        """Subscribe this connection's inbox if needed; returns its subject prefix."""
        inbox = self._inbox
        if inbox is None:
            inbox = _Inbox(f"{INBOX_PREFIX}.{secrets.token_hex(12)}")
            inbox.ready = asyncio.get_running_loop().create_task(self._subscribe_inbox(inbox))
            self._inbox = inbox
        assert inbox.ready is not None
        await asyncio.shield(inbox.ready)
        return inbox.prefix

    async def _subscribe_inbox(self, inbox: _Inbox) -> None:
        try:
            await self._call(m.CoreSubscribe(f"{inbox.prefix}.*", None), m.SubscribeOk, sink=_InboxSink(self, inbox))
        except BaseException:
            if self._inbox is inbox:
                self._inbox = None  # the next request tries again
            raise

    def _on_reply(self, msg: m.CoreMsg) -> None:
        token = msg.subject.rsplit(".", 1)[-1]
        fut = self._replies.pop(token, None)
        if fut is not None and not fut.done():  # else late (timed out) or duplicate
            fut.set_result(CoreMessage(msg, self._host))

    def _drop_inbox(self, err: Exception) -> None:
        """Forget the inbox (the next request subscribes a new one) and fail the requests waiting on it."""
        inbox, self._inbox = self._inbox, None
        if inbox is not None and inbox.conn is not None and not inbox.conn.closed:
            inbox.conn.remove_sub(inbox.sub_id)
        waiting = list(self._replies.values())
        self._replies.clear()
        for fut in waiting:
            if not fut.done():
                fut.set_exception(err)

    # ---- key-value buckets ----------------------------------------------------------

    def kv(self, bucket: str) -> KvBucket:
        """A handle to the key-value bucket ``bucket`` (create it with :meth:`KvBucket.create`)."""
        return KvBucket(self, bucket)

    # ---- plumbing -------------------------------------------------------------------

    def _flush_acks(self) -> None:
        self._ack_flush_scheduled = False
        if not self._pending_acks:
            return
        acks, self._pending_acks = self._pending_acks, {}
        if self._state != "connected":
            return
        for consumer, offsets in acks.items():
            self._conn.send(m.Ack(consumer, offsets))

    def _check_open(self) -> None:
        if self._state == "closed":
            raise ConnectionError("client is closed")
        if self._state == "reconnecting":
            raise ConnectionError("not connected (reconnecting)")

    def send_request(self, req: m.Request) -> asyncio.Future[m.Response]:
        """Low level: write a protocol request now and return the future of its
        response (queued acks are flushed first)."""
        self._check_open()
        self._flush_acks()
        return self._conn.send_request(req)

    async def raw_request(
        self, req: m.Request, *, timeout: float | None = None, sink: SubscriptionSink | None = None
    ) -> m.Response:
        """Low level: send any protocol request on the current connection and
        return its response. Error responses raise :class:`ServerError`."""
        self._check_open()
        self._flush_acks()
        return await self._conn.request(req, timeout=timeout, sink=sink)

    async def _call(
        self, req: m.Request, cls: type[R], *, timeout: float | None = None, sink: SubscriptionSink | None = None
    ) -> R:
        resp = await self.raw_request(req, timeout=timeout, sink=sink)
        if not isinstance(resp, cls):
            raise ProtocolError(f"unexpected reply to {type(req).__name__}: {type(resp).__name__}")
        return resp

    async def _json(self, req: m.Request) -> dict[str, Any]:
        r = await self._call(req, m.Json)
        v = parse_json(r.data, type(req).__name__)
        if not isinstance(v, dict):
            raise ProtocolError(f"bad JSON in reply to {type(req).__name__}: expected an object")
        return v

    async def _json_list(self, req: m.Request) -> list[Any]:
        r = await self._call(req, m.Json)
        v = parse_json(r.data, type(req).__name__)
        if not isinstance(v, list):
            raise ProtocolError(f"bad JSON in reply to {type(req).__name__}: expected an array")
        return v

    def _on_connection_lost(self, conn: Connection, err: Exception) -> None:
        if conn is not self._conn or self._state != "connected":
            return
        # Responses to the inbox can't arrive any more; a request after the
        # reconnect subscribes a new inbox.
        self._drop_inbox(ConnectionError(f"connection lost: {err}"))
        if self._reconnect_opts is None:
            self._state = "closed"
            for sub in list(self._subs):
                sub.end(EndReason(ErrorCode.UNAVAILABLE, "connection closed"), keep_buffered=False)
            for core in list(self._core_subs):
                core.end(EndReason(ErrorCode.UNAVAILABLE, "connection closed"), keep_buffered=False)
            self._emit("close", err)
            return
        self._state = "reconnecting"
        self._pending_acks.clear()  # those records will be redelivered
        for sub in self._subs:
            sub.suspend()
        for core in self._core_subs:
            core.suspend()
        self._emit("disconnect", err)
        self._reconnect_task = asyncio.get_running_loop().create_task(self._reconnect_loop(self._reconnect_opts))

    async def _reconnect_loop(self, opts: ReconnectOptions) -> None:
        last_err: Exception = ConnectionError("connection lost")
        attempt = 0
        while opts.max_attempts is None or attempt < opts.max_attempts:
            attempt += 1
            delay = min(opts.initial_delay * 2 ** min(attempt - 1, 30), opts.max_delay)
            await asyncio.sleep(delay * random.uniform(0.75, 1.25))
            if self._state != "reconnecting":
                return  # closed meanwhile
            try:
                conn = await _open_leader(
                    self._opts, self._servers, self._conn.info.leader, self._conn_closed, self._conn_async_error
                )
            except ServerError as e:
                last_err = e
                # A rejected credential won't get better by retrying.
                if e.code in (ErrorCode.UNAUTHORIZED, ErrorCode.FORBIDDEN):
                    break
                continue
            except Exception as e:
                last_err = e
                continue
            if self._state != "reconnecting":
                await conn.close()
                return
            self._conn = conn
            self._state = "connected"
            await self._restore(conn)
            if conn is self._conn and self._state == "connected" and not conn.closed:
                self._reconnect_task = None
                self._emit("reconnect", conn.info)
            return
        if self._state != "reconnecting":
            return
        self._state = "closed"
        self._reconnect_task = None
        reason = EndReason(ErrorCode.UNAVAILABLE, f"connection lost: {last_err}")
        for sub in list(self._subs):
            sub.end(reason, keep_buffered=False)
        for core in list(self._core_subs):
            core.end(reason, keep_buffered=False)
        self._emit("close", last_err)

    def _conn_closed(self, conn: Connection, err: Exception) -> None:
        self._on_connection_lost(conn, err)

    def _conn_async_error(self, err: ExspeedError) -> None:
        self._emit("error", err)

    async def _restore(self, conn: Connection) -> None:
        """Re-create ephemeral consumers, then re-subscribe every live subscription (consumer and core)."""
        for spec in list(self._ephemeral.values()):
            try:
                await conn.request(m.CreateConsumer(spec.to_wire()))
            except ConnectionError:
                return  # lost again; the next loop retries
            except Exception as e:
                log.warning("re-creating ephemeral consumer %r failed: %s", spec.name, e)

        async def resubscribe(sub: Subscription) -> None:
            try:
                await conn.request(m.Subscribe(sub.consumer, sub.window), sink=sub)
            except ConnectionError:
                return  # stays suspended for the next attempt
            except Exception as e:
                code = e.code if isinstance(e, ServerError) else ErrorCode.INTERNAL
                sub.end(EndReason(code, e.message if isinstance(e, ServerError) else str(e)), keep_buffered=False)

        async def resubscribe_core(sub: CoreSubscription) -> None:
            try:
                await conn.request(m.CoreSubscribe(sub.subject, sub.queue), sink=sub)
            except ConnectionError:
                return
            except Exception as e:
                code = e.code if isinstance(e, ServerError) else ErrorCode.INTERNAL
                sub.end(EndReason(code, e.message if isinstance(e, ServerError) else str(e)), keep_buffered=True)

        await asyncio.gather(
            *(resubscribe(s) for s in list(self._subs)),
            *(resubscribe_core(s) for s in list(self._core_subs)),
        )

    def __repr__(self) -> str:
        return f"ExspeedClient({self._conn.opts.host}:{self._conn.opts.port}, state={self._state})"


def connect(
    host: str = "127.0.0.1",
    port: int = DEFAULT_PORT,
    *,
    servers: Iterable[str] | None = None,
    token: str | None = None,
    tls: bool | ssl.SSLContext | TlsOptions | None = None,
    client_id: str = "exspeed-py",
    request_timeout: float = 30.0,
    keepalive: float = 20.0,
    reconnect: bool | ReconnectOptions = True,
    verify_crc: bool = False,
) -> AwaitableContext[ExspeedClient]:
    """Connect to an Exspeed server; same as :meth:`ExspeedClient.connect`.

    ``client = await exspeed.connect(...)`` or ``async with exspeed.connect(...) as client``.
    """
    return ExspeedClient.connect(
        host,
        port,
        servers=servers,
        token=token,
        tls=tls,
        client_id=client_id,
        request_timeout=request_timeout,
        keepalive=keepalive,
        reconnect=reconnect,
        verify_crc=verify_crc,
    )


def _parse_addr(addr: str, fallback_port: int) -> tuple[str, int]:
    i = addr.rfind(":")
    if i <= 0 or (addr.startswith("[") and not addr[:i].endswith("]")):
        return addr.strip("[]"), fallback_port
    try:
        port = int(addr[i + 1 :])
    except ValueError:
        port = fallback_port
    return addr[:i].strip("[]"), port


async def _open_leader(
    opts: ConnectionOptions,
    servers: list[str],
    hint: str | None,
    on_close: Callable[[Connection, Exception], None],
    on_async_error: Callable[[ExspeedError], None],
) -> Connection:
    """Open a connection to the cluster leader.

    Without seed ``servers`` this is a plain connect to ``opts.host:opts.port``,
    except that a node naming another node as leader in its handshake is
    followed. With seeds, each candidate (the last known leader first) is asked
    whether it leads; leader hints are followed, and a follower is accepted
    only when no node claims to lead.
    """
    queue: list[str] = []
    if hint:
        queue.append(hint)
    queue.extend(servers or [f"{opts.host}:{opts.port}"])
    tried: set[str] = set()
    fallback: Connection | None = None
    conn: Connection | None = None
    last_err: Exception | None = None
    try:
        while queue:
            addr = queue.pop(0)
            if addr in tried:
                continue
            tried.add(addr)
            host, port = _parse_addr(addr, opts.port)
            conn = None
            try:
                conn = await Connection.open(replace(opts, host=host, port=port), on_close, on_async_error)
            except ServerError as e:
                if e.code in (ErrorCode.UNAUTHORIZED, ErrorCode.FORBIDDEN):
                    raise
                last_err = e
                continue
            except ExspeedError as e:
                last_err = e
                continue
            is_leader = conn.info.leader is None
            leader = conn.info.leader
            if servers:
                try:
                    resp = await conn.request(m.Metadata())
                    if isinstance(resp, m.Json):
                        md = parse_json(resp.data, "Metadata")
                        if isinstance(md, dict):
                            is_leader = md.get("is_leader") is True
                            leader = md.get("leader") if isinstance(md.get("leader"), str) else None
                except ExspeedError:
                    pass  # trust the handshake
            if is_leader:
                found, conn = conn, None
                if fallback is not None:
                    await fallback.close()
                return found
            if leader and leader not in tried:
                queue.insert(0, leader)
            if fallback is None:
                fallback, conn = conn, None
            else:
                await conn.close()
                conn = None
    except BaseException:
        # Failed or cancelled: don't leak the connections opened so far.
        for c in (conn, fallback):
            if c is not None:
                c.discard()
        raise
    if fallback is not None:
        return fallback
    raise last_err or ConnectionError("no server reachable")
