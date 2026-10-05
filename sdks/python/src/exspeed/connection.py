"""One TCP (or TLS) connection: handshake, frame parsing, correlation-id
multiplexing, push routing and keepalive. Reconnection lives one level up, in
:class:`exspeed.ExspeedClient`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import ssl
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .errors import ConnectionError, ExspeedError, ProtocolError, ServerError, TimeoutError
from .protocol import messages as m
from .protocol.codec import FrameParser, WireRecord
from .types import ServerInfo

__all__ = ["Connection", "ConnectionOptions", "SubscriptionSink", "error_from_response"]


@dataclass
class ConnectionOptions:
    """Settings of one connection."""

    host: str
    port: int
    client_id: str
    token: str | None
    #: ``None`` for plain TCP.
    ssl_context: ssl.SSLContext | None
    server_hostname: str | None
    #: Seconds; also bounds opening the connection.
    request_timeout: float
    #: Seconds between pings; 0 disables.
    keepalive: float
    verify_crc: bool = False


class SubscriptionSink:
    """Receives a subscription's pushes (``Deliver`` for consumers, ``CoreMsg`` for core subscriptions)."""

    def on_subscribed(self, conn: Connection, sub_id: int) -> None:
        """Called synchronously when ``SubscribeOk`` arrives, before any push for it is routed."""

    def on_deliver(self, records: list[WireRecord]) -> None:
        """A ``Deliver`` push."""

    def on_core_msg(self, msg: m.CoreMsg) -> None:
        """A ``CoreMsg`` push."""

    def on_ended(self, code: int, message: str) -> None:
        """A ``SubscriptionEnded`` push."""


def error_from_response(resp: m.Error) -> ServerError:
    """The :class:`ServerError` for an ``Error`` frame (its detail decoded as JSON when it is JSON)."""
    detail: Any = None
    if resp.detail:
        try:
            detail = json.loads(resp.detail)
        except ValueError:
            detail = resp.detail.decode("utf-8", "replace")
    return ServerError(resp.code, resp.message, detail)


@dataclass
class _Pending:
    fut: asyncio.Future[m.Response]
    timer: asyncio.TimerHandle | None
    sink: SubscriptionSink | None


class Connection(asyncio.Protocol):
    """A protocol-v2 connection. Create one with :meth:`open`."""

    def __init__(
        self,
        opts: ConnectionOptions,
        on_close: Callable[[Connection, Exception], None],
        on_async_error: Callable[[ExspeedError], None],
    ) -> None:
        self.opts = opts
        self._on_close = on_close
        self._on_async_error = on_async_error
        self._parser = FrameParser()
        self._pending: dict[int, _Pending] = {}
        self._subs: dict[int, SubscriptionSink] = {}
        self._next_corr = 1
        self._transport: asyncio.Transport | None = None
        self._closed = False
        self._closed_by_user = False
        self._established = False
        self._info: ServerInfo | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._paused = False
        self._drain_waiters: list[asyncio.Future[None]] = []
        self._lost = asyncio.Event()

    # ---- lifecycle ------------------------------------------------------------

    @classmethod
    async def open(
        cls,
        opts: ConnectionOptions,
        on_close: Callable[[Connection, Exception], None],
        on_async_error: Callable[[ExspeedError], None],
    ) -> Connection:
        """Open a socket and run the ``Connect`` handshake.

        ``on_close`` is called once if an established connection closes for
        any reason other than :meth:`close`.
        """
        loop = asyncio.get_running_loop()
        conn = cls(opts, on_close, on_async_error)
        where = f"{opts.host}:{opts.port}"
        tls = opts.ssl_context
        try:
            await asyncio.wait_for(
                loop.create_connection(
                    lambda: conn,
                    opts.host,
                    opts.port,
                    ssl=tls,
                    server_hostname=(opts.server_hostname or opts.host) if tls else None,
                    ssl_handshake_timeout=opts.request_timeout if tls else None,
                ),
                opts.request_timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionError(f"connect to {where} timed out") from None
        except (OSError, ssl.SSLError, ValueError) as e:
            raise ConnectionError(f"connect to {where} failed: {e}") from None
        try:
            resp = await conn.request(m.Connect(opts.client_id, opts.token))
            if not isinstance(resp, m.ConnectOk):
                raise ProtocolError(f"unexpected handshake reply {type(resp).__name__}")
            conn._info = ServerInfo(resp.server_version, resp.node_id, resp.leader)
        except BaseException as e:
            conn._closed_by_user = True
            conn._teardown(e if isinstance(e, ExspeedError) else ConnectionError("handshake aborted"))
            raise
        if conn._closed:
            raise ConnectionError(f"connection to {where} closed during the handshake")
        conn._established = True
        conn._start_keepalive()
        return conn

    @property
    def info(self) -> ServerInfo:
        """Handshake info."""
        assert self._info is not None
        return self._info

    @property
    def closed(self) -> bool:
        """True once the connection is closed (or lost)."""
        return self._closed

    async def close(self) -> None:
        """Close gracefully: flush what was written (acks), then drop the socket."""
        if self._closed:
            return
        self._closed_by_user = True
        t = self._transport
        if t is not None and not t.is_closing():
            t.close()
            try:
                await asyncio.wait_for(self._lost.wait(), 1.0)
            except asyncio.TimeoutError:
                t.abort()
        self._teardown(ConnectionError("connection closed"))

    def abort(self) -> None:
        """Drop the socket at once (as if the connection were lost)."""
        if self._transport is not None:
            self._transport.abort()

    def discard(self) -> None:
        """Drop the socket at once without reporting the close to ``on_close``."""
        self._closed_by_user = True
        self.abort()
        self._teardown(ConnectionError("connection closed"))

    # ---- requests -------------------------------------------------------------

    def send_request(
        self,
        req: m.Request,
        *,
        timeout: float | None = None,
        sink: SubscriptionSink | None = None,
    ) -> asyncio.Future[m.Response]:
        """Write a request now and return the future of its response.

        The frame is written before this returns, so the wire order of
        requests matches the order of calls. Error responses fail the future
        with :class:`ServerError`; no response within ``timeout`` seconds
        (default: the request timeout) fails it with :class:`TimeoutError`.
        """
        if self._closed or self._transport is None:
            raise ConnectionError("connection closed")
        corr = self._alloc_corr()
        frame = m.request_frame(req, corr)
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[m.Response] = loop.create_future()
        t = self.opts.request_timeout if timeout is None else timeout
        name = type(req).__name__
        timer = loop.call_later(t, self._on_timeout, corr, name, t) if t > 0 else None
        self._pending[corr] = _Pending(fut, timer, sink)
        fut.add_done_callback(lambda f: self._forget(corr, f))
        self._transport.write(frame)
        return fut

    async def request(
        self,
        req: m.Request,
        *,
        timeout: float | None = None,
        sink: SubscriptionSink | None = None,
    ) -> m.Response:
        """Send a request and wait for its response (see :meth:`send_request`)."""
        fut = self.send_request(req, timeout=timeout, sink=sink)
        if self._paused:
            await self._drain()
        return await fut

    def send(self, req: m.Request) -> bool:
        """Send with correlation id 0 (fire-and-forget): no reply on success; a
        failure arrives as an async error. Returns False when not sent."""
        if self._closed or self._transport is None:
            return False
        self._transport.write(m.request_frame(req, 0))
        return True

    def remove_sub(self, sub_id: int) -> None:
        """Stop routing pushes for a subscription."""
        self._subs.pop(sub_id, None)

    def _alloc_corr(self) -> int:
        c = self._next_corr
        self._next_corr = 1 if c >= 0xFFFFFFFF else c + 1
        return c

    def _on_timeout(self, corr: int, name: str, t: float) -> None:
        p = self._pending.pop(corr, None)
        if p is not None and not p.fut.done():
            p.fut.set_exception(TimeoutError(f"{name} timed out after {t:g} s"))

    def _forget(self, corr: int, fut: asyncio.Future[m.Response]) -> None:
        p = self._pending.get(corr)
        if p is not None and p.fut is fut:
            del self._pending[corr]
            if p.timer is not None:
                p.timer.cancel()

    async def _drain(self) -> None:
        if not self._paused or self._closed:
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._drain_waiters.append(fut)
        await fut

    # ---- asyncio.Protocol -------------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self._transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    def data_received(self, data: bytes) -> None:
        try:
            frames = self._parser.push(data)
        except ProtocolError as e:
            # The byte stream can't be resynchronised: drop the connection.
            self.abort()
            self._teardown(e)
            return
        for f in frames:
            if self._closed:
                return
            try:
                resp = m.decode_response(f.opcode, f.payload, verify_crc=self.opts.verify_crc)
            except ProtocolError as e:
                p = self._pending.pop(f.correlation_id, None) if f.correlation_id else None
                if p is not None and not p.fut.done():
                    p.fut.set_exception(e)
                else:
                    self._on_async_error(e)
                continue
            self._route(f.correlation_id, resp)

    def connection_lost(self, exc: Exception | None) -> None:
        self._lost.set()
        msg = f"connection lost: {exc}" if exc else "connection closed"
        self._teardown(ConnectionError(msg))

    def pause_writing(self) -> None:
        self._paused = True

    def resume_writing(self) -> None:
        self._paused = False
        self._wake_drainers(None)

    def _wake_drainers(self, err: Exception | None) -> None:
        waiters, self._drain_waiters = self._drain_waiters, []
        for w in waiters:
            if not w.done():
                if err is None:
                    w.set_result(None)
                else:
                    w.set_exception(err)

    # ---- routing --------------------------------------------------------------

    def _route(self, corr: int, resp: m.Response) -> None:
        if corr == 0:
            if isinstance(resp, m.Deliver):
                sink = self._subs.get(resp.sub_id)
                if sink is not None:
                    sink.on_deliver(resp.records)
            elif isinstance(resp, m.CoreMsg):
                sink = self._subs.get(resp.sub_id)
                if sink is not None:
                    sink.on_core_msg(resp)
            elif isinstance(resp, m.SubscriptionEnded):
                sink = self._subs.pop(resp.sub_id, None)
                if sink is not None:
                    sink.on_ended(resp.code, resp.message)
            elif isinstance(resp, m.Error):
                self._on_async_error(error_from_response(resp))
            return
        p = self._pending.pop(corr, None)
        if p is not None and p.timer is not None:
            p.timer.cancel()
        if isinstance(resp, m.SubscribeOk):
            if p is not None and p.sink is not None and not p.fut.done():
                # Register before resolving so a Deliver in the same chunk is not lost.
                self._subs[resp.sub_id] = p.sink
                p.sink.on_subscribed(self, resp.sub_id)
            else:
                # The subscribe call timed out or was abandoned; release the server side.
                self.send(m.Unsubscribe(resp.sub_id))
        if p is None or p.fut.done():
            return
        if isinstance(resp, m.Error):
            p.fut.set_exception(error_from_response(resp))
        else:
            p.fut.set_result(resp)

    # ---- keepalive and teardown -----------------------------------------------

    def _start_keepalive(self) -> None:
        if self.opts.keepalive > 0:
            self._keepalive_task = asyncio.get_running_loop().create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self.opts.keepalive)
            if self._closed:
                return
            try:
                await self.request(m.Ping())
            except TimeoutError:
                # A ping that times out means the peer is gone (half-open socket).
                self.abort()
                return
            except ExspeedError:
                return

    def _teardown(self, err: Exception) -> None:
        if self._closed:
            return
        self._closed = True
        task = self._keepalive_task
        self._keepalive_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        t = self._transport
        if t is not None and not t.is_closing():
            t.abort()
        pending = list(self._pending.values())
        self._pending.clear()
        self._subs.clear()
        conn_err = err if isinstance(err, ConnectionError) else ConnectionError(str(err))
        for p in pending:
            if p.timer is not None:
                p.timer.cancel()
            if not p.fut.done():
                p.fut.set_exception(conn_err)
        self._wake_drainers(conn_err)
        if self._established and not self._closed_by_user:
            self._on_close(self, err)
