"""A scriptable protocol-v2 server for unit tests."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from exspeed.protocol import FrameParser, decode_request, response_frame
from exspeed.protocol import messages as m

T = TypeVar("T")


@dataclass
class Received:
    corr: int
    req: Any


class FakeConn(asyncio.Protocol):
    """One accepted client connection."""

    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.received: list[Received] = []
        self.parser = FrameParser()
        self.transport: asyncio.Transport | None = None
        self.closed = False

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport
        self.server.conns.append(self)

    def data_received(self, data: bytes) -> None:
        for f in self.parser.push(data):
            try:
                req = decode_request(f.opcode, f.payload)
            except Exception as e:
                self.reply(f.correlation_id, m.Error(400, str(e), None))
                continue
            self.received.append(Received(f.correlation_id, req))
            self.server.handle(self, f.correlation_id, req)

    def connection_lost(self, exc: Exception | None) -> None:
        self.closed = True

    def reply(self, corr: int, resp: m.Response) -> None:
        if self.transport is not None and not self.transport.is_closing():
            self.transport.write(response_frame(resp, corr))

    def reply_many(self, frames: list[tuple[int, m.Response]]) -> None:
        """Write several frames in a single chunk."""
        if self.transport is not None:
            self.transport.write(b"".join(response_frame(r, c) for c, r in frames))

    def of(self, cls: type[T]) -> list[tuple[int, T]]:
        """Received requests of one type, as ``(corr, req)``."""
        return [(r.corr, r.req) for r in self.received if isinstance(r.req, cls)]

    def types(self) -> list[str]:
        return [type(r.req).__name__ for r in self.received]

    def destroy(self) -> None:
        if self.transport is not None:
            self.transport.abort()

    def end(self) -> None:
        if self.transport is not None:
            self.transport.close()


Handler = Callable[[FakeConn, int, Any], "bool | None"]


class FakeServer:
    """Connect and Ping are answered automatically unless ``handler`` returns True for them."""

    def __init__(self) -> None:
        self.conns: list[FakeConn] = []
        self.handler: Handler = lambda conn, corr, req: None
        self._server: asyncio.Server | None = None

    @classmethod
    async def start(cls, handler: Handler | None = None) -> FakeServer:
        fake = cls()
        if handler is not None:
            fake.handler = handler
        loop = asyncio.get_running_loop()
        fake._server = await loop.create_server(lambda: FakeConn(fake), "127.0.0.1", 0)
        return fake

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(self._server.sockets[0].getsockname()[1])

    @property
    def last(self) -> FakeConn:
        return self.conns[-1]

    def handle(self, conn: FakeConn, corr: int, req: Any) -> None:
        if self.handler(conn, corr, req) is True:
            return
        if isinstance(req, m.Connect):
            conn.reply(corr, m.ConnectOk("test", "n1", None))
        elif isinstance(req, m.Ping):
            conn.reply(corr, m.Pong())

    async def until(self, cond: Callable[[], object], timeout: float = 2.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not cond():
            if loop.time() > deadline:
                raise AssertionError("FakeServer.until: timed out")
            await asyncio.sleep(0.005)

    async def close(self) -> None:
        for c in self.conns:
            c.destroy()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
