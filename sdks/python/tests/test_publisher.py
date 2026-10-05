"""The coalescing publisher against the fake server."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from fake_server import FakeConn, FakeServer

from exspeed import ExspeedClient, PublishResult, ServerError
from exspeed.protocol import messages as m


class AutoAck:
    """Acknowledge publishes with increasing offsets."""

    def __init__(self) -> None:
        self.next_offset = 0

    def __call__(self, conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Publish):
            conn.reply(corr, m.PublishOk(self.next_offset, False))
            self.next_offset += 1
        if isinstance(req, m.PublishBatch):
            results = [(self.next_offset + i, False) for i in range(len(req.records))]
            self.next_offset += len(req.records)
            conn.reply(corr, m.PublishBatchOk(results))


Setup = Callable[..., Any]


@pytest.fixture
async def setup(server: FakeServer) -> AsyncIterator[Callable[..., Any]]:
    clients: list[ExspeedClient] = []

    async def go(handler: Any = None) -> ExspeedClient:
        server.handler = handler if handler is not None else AutoAck()
        c = await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=False)
        clients.append(c)
        return c

    yield go
    for c in clients:
        await c.close()


def subjects(reqs: list[Any]) -> list[str]:
    out: list[str] = []
    for r in reqs:
        if isinstance(r, m.PublishBatch):
            out.extend(x.subject for x in r.records)
        elif isinstance(r, m.Publish):
            out.append(r.record.subject)
    return out


def sent(server: FakeServer) -> list[Any]:
    return [r.req for r in server.last.received[1:]]


async def test_coalesces_publishes_from_one_tick_into_a_single_batch(server: FakeServer, setup: Setup) -> None:
    client = await setup()
    p = client.publisher()
    results = await asyncio.gather(*(p.publish("s", f"n.{i}", str(i)) for i in range(100)))
    assert [r.offset for r in results] == list(range(100))
    reqs = sent(server)
    assert [type(r).__name__ for r in reqs] == ["PublishBatch"]
    assert subjects(reqs) == [f"n.{i}" for i in range(100)]


async def test_sends_a_lone_record_as_a_plain_publish(server: FakeServer, setup: Setup) -> None:
    client = await setup()
    r = await client.publisher().publish("s", "one", "x", headers={"a": "b"}, priority=3)
    assert r == PublishResult(0, False)
    req = server.last.received[1].req
    assert isinstance(req, m.Publish)
    assert req.record.headers == [("a", "b"), ("exspeed-priority", "3")]


async def test_splits_by_max_batch_records_and_by_stream_keeping_order(server: FakeServer, setup: Setup) -> None:
    client = await setup()
    p = client.publisher(max_batch_records=3)
    calls = [p.publish("a", f"a.{i}") for i in range(4)]
    calls += [p.publish("b", f"b.{i}") for i in range(2)]
    calls.append(p.publish("a", "a.4"))
    await asyncio.gather(*calls)
    reqs = sent(server)
    shape = [(type(r).__name__, r.stream, len(r.records) if isinstance(r, m.PublishBatch) else 1) for r in reqs]
    # A full queue (3 records) is flushed at once, split into same-stream runs.
    assert shape == [
        ("PublishBatch", "a", 3),
        ("Publish", "a", 1),
        ("PublishBatch", "b", 2),
        ("Publish", "a", 1),
    ]
    assert subjects(reqs) == ["a.0", "a.1", "a.2", "a.3", "b.0", "b.1", "a.4"]


async def test_waits_for_a_batch_window_before_sending(server: FakeServer, setup: Setup) -> None:
    client = await setup()
    p = client.publisher(batch_window=0.03)
    a = asyncio.ensure_future(p.publish("s", "x", "1"))
    await asyncio.sleep(0.005)
    b = asyncio.ensure_future(p.publish("s", "y", "2"))
    await asyncio.gather(a, b)
    assert [type(r).__name__ for r in sent(server)] == ["PublishBatch"]


async def test_bounds_records_in_flight_and_keeps_order_while_waiting(server: FakeServer, setup: Setup) -> None:
    held: list[tuple[FakeConn, int, Any]] = []

    def hold(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, (m.Publish, m.PublishBatch)):
            held.append((conn, corr, req))

    client = await setup(hold)
    ack = AutoAck()
    p = client.publisher(max_in_flight=2)
    tasks = [asyncio.ensure_future(p.publish("s", f"n.{i}")) for i in range(5)]
    await server.until(lambda: len(held) == 1)
    await asyncio.sleep(0.03)
    assert len(held) == 1  # first two only
    assert p.pending == 2
    while held or p.pending > 0:
        if held:
            ack(*held.pop(0))
        await asyncio.sleep(0.01)
    await asyncio.gather(*tasks)
    assert subjects([r.req for r in server.last.received]) == [f"n.{i}" for i in range(5)]


async def test_rejects_every_record_of_a_failed_batch(server: FakeServer, setup: Setup) -> None:
    def fail(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.PublishBatch):
            conn.reply(corr, m.Error(403, "forbidden", None))

    client = await setup(fail)
    p = client.publisher()
    results = await asyncio.gather(p.publish("s", "a"), p.publish("s", "b"), return_exceptions=True)
    for r in results:
        assert isinstance(r, ServerError) and r.code == 403
    await p.flush()
    assert p.pending == 0


async def test_flush_waits_for_everything_accepted_close_rejects_later_publishes(
    server: FakeServer, setup: Setup
) -> None:
    client = await setup()
    p = client.publisher()
    done: list[int] = []

    async def one() -> None:
        done.append((await p.publish("s", "x")).offset)

    tasks = [asyncio.ensure_future(one()) for _ in range(10)]
    await asyncio.sleep(0)
    await p.flush()
    assert p.pending == 0
    await asyncio.gather(*tasks)
    assert sorted(done) == list(range(10))
    async with p:
        pass  # closes on exit
    with pytest.raises(Exception, match="closed"):
        await p.publish("s", "x")


async def test_fails_publishes_when_the_client_is_closed(server: FakeServer, setup: Setup) -> None:
    client = await setup()
    p = client.publisher()
    await client.close()
    with pytest.raises(Exception, match="closed"):
        await p.publish("s", "x")
    assert p.pending == 0
