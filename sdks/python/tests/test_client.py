"""Connection logic against the scriptable fake server: handshake,
correlation, timeouts, subscriptions and credit, acks, reconnection,
keepalive and leader discovery."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from fake_server import FakeConn, FakeServer

import exspeed
from exspeed import (
    ConnectionError,
    ExspeedClient,
    Message,
    PublishRecord,
    ReconnectOptions,
    ServerError,
    TimeoutError,
)
from exspeed.protocol import OpCode, WireRecord, encode_frame
from exspeed.protocol import messages as m


def rec(offset: int, value: str | None = None) -> WireRecord:
    return WireRecord(
        offset=offset,
        timestamp_ns=1_700_000_000_000_000_000 + offset,
        delivery_count=1,
        subject="orders.placed",
        key=None,
        value=(value if value is not None else f"v{offset}").encode(),
        headers=[("h", "1")],
    )


Connect = Callable[..., Awaitable[ExspeedClient]]


@pytest.fixture
async def connect(server: FakeServer) -> AsyncIterator[Connect]:
    clients: list[ExspeedClient] = []

    async def factory(**kw: Any) -> ExspeedClient:
        opts: dict[str, Any] = {"port": server.port, "keepalive": 0, "reconnect": False, **kw}
        c = await ExspeedClient.connect(**opts)
        clients.append(c)
        return c

    yield factory
    for c in clients:
        await c.close()


# ---------------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------------


async def test_sends_client_id_and_token_and_exposes_server_info(server: FakeServer, connect: Connect) -> None:
    c = await connect(token="secret", client_id="unit")
    corr, req = server.last.received[0].corr, server.last.received[0].req
    assert req == m.Connect("unit", "secret")
    assert corr != 0
    assert c.server_info == exspeed.ServerInfo("test", "n1", None)
    assert c.connected


async def test_rejects_with_server_error_401_when_the_token_is_refused(server: FakeServer) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if not isinstance(req, m.Connect):
            return None
        conn.reply(corr, m.Error(401, "unauthorized", None))
        conn.end()
        return True

    server.handler = handler
    with pytest.raises(ServerError) as e:
        await ExspeedClient.connect(port=server.port, token="bad")
    assert e.value.code == 401


async def test_fails_with_connection_error_when_nothing_listens() -> None:
    s = await FakeServer.start()
    port = s.port
    await s.close()
    with pytest.raises(ConnectionError):
        await ExspeedClient.connect(port=port)
    # The typed error is also the built-in ConnectionError.
    with pytest.raises(builtins_connection_error()):
        await ExspeedClient.connect(port=port)


def builtins_connection_error() -> type[BaseException]:
    import builtins

    return builtins.ConnectionError


async def test_connect_as_async_context_manager_closes_on_exit(server: FakeServer) -> None:
    async with exspeed.connect(port=server.port, keepalive=0) as c:
        assert c.connected
        await c.ping()
    assert not c.connected
    await server.until(lambda: server.last.closed)


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


async def test_matches_out_of_order_responses_by_correlation_id(server: FakeServer, connect: Connect) -> None:
    c = await connect()
    server.handler = lambda conn, corr, req: True  # answer manually
    a = asyncio.ensure_future(c.publish("s", "a", "1"))
    b = asyncio.ensure_future(c.publish("s", "b", "2"))
    await server.until(lambda: len(server.last.of(m.Publish)) == 2)
    (ca, _), (cb, _) = server.last.of(m.Publish)
    server.last.reply(cb, m.PublishOk(2, False))
    server.last.reply(ca, m.PublishOk(1, True))
    assert await a == exspeed.PublishResult(1, True)
    assert await b == exspeed.PublishResult(2, False)


async def test_encodes_values_bytes_as_is_strings_utf8_objects_json(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.PublishBatch):
            conn.reply(corr, m.PublishBatchOk([(i, False) for i in range(len(req.records))]))

    server.handler = handler
    results = await c.publish_batch(
        "s",
        [
            PublishRecord("a", b"\x01\x02"),
            PublishRecord("a", "hé"),
            PublishRecord("a", {"id": 1}, key="k", headers={"x": "y"}, msg_id="m1"),
        ],
    )
    assert [r.offset for r in results] == [0, 1, 2]
    recs = server.last.of(m.PublishBatch)[0][1].records
    assert recs[0].value == b"\x01\x02"
    assert recs[1].value.decode() == "hé"
    assert recs[2].value == b'{"id":1}'
    assert recs[2].key == b"k"
    assert recs[2].headers == [("x", "y")]
    assert recs[2].msg_id == "m1"
    assert await c.publish_batch("s", []) == []


async def test_surfaces_code_message_detail_and_leader_hint(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.StreamInfo):
            conn.reply(corr, m.Error(503, "not the leader", b'{"leader":"h2:5933"}'))

    server.handler = handler
    with pytest.raises(ServerError) as e:
        await c.stream_info("s")
    err = e.value
    assert err.code == 503
    assert err.message == "not the leader"
    assert err.detail == {"leader": "h2:5933"}
    assert err.leader_hint == "h2:5933"
    assert str(err) == "ServerError 503: not the leader"


async def test_times_out_requests_the_server_never_answers(server: FakeServer, connect: Connect) -> None:
    c = await connect(request_timeout=0.1)
    server.handler = lambda conn, corr, req: True
    with pytest.raises(TimeoutError):
        await c.metadata()
    import builtins

    with pytest.raises(builtins.TimeoutError):
        await c.metadata()


async def test_gives_pull_and_read_extra_time_for_their_server_side_wait(server: FakeServer, connect: Connect) -> None:
    c = await connect(request_timeout=0.05)
    loop = asyncio.get_running_loop()

    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Pull):
            loop.call_later(0.15, conn.reply, corr, m.Messages([rec(3)]))
            return True
        if isinstance(req, m.Read):
            loop.call_later(0.15, conn.reply, corr, m.ReadResult(5, 5, [rec(4)]))
            return True
        return None

    server.handler = handler
    msgs = await c.pull("c", expires=0.2)
    assert [x.offset for x in msgs] == [3]
    assert server.last.of(m.Pull)[0][1] == m.Pull("c", 100, 0, 200)
    r = await c.read("s", from_offset=4, wait=0.2, filter="orders.*")
    assert [x.offset for x in r.records] == [4]
    assert (r.next_offset, r.high_watermark) == (5, 5)
    assert server.last.of(m.Read)[0][1] == m.Read("s", 4, 100, 0, 200, "orders.*")


async def test_parses_json_replies(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Metadata):
            conn.reply(corr, m.Json(b'{"node_id":"n1","is_leader":true,"leader":null,"server_version":"x"}'))
        elif isinstance(req, m.StreamInfo):
            info = {
                "name": "s",
                "earliest_offset": 2,
                "next_offset": 7,
                "records": 5,
                "internal": False,
                "config": {"max_age_secs": 60, "max_msgs": 3, "discard": "new", "retention": "work_queue"},
            }
            conn.reply(corr, m.Json(json.dumps(info).encode()))
        elif isinstance(req, m.Query):
            conn.reply(
                corr,
                m.Json(b'{"columns":["n"],"rows":[[3]],"row_count":1,"execution_time_ms":2,"truncated":false}'),
            )

    server.handler = handler
    assert await c.metadata() == exspeed.Metadata("n1", True, None, "x")
    info = await c.stream_info("s")
    assert (info.name, info.earliest_offset, info.next_offset, info.records) == ("s", 2, 7, 5)
    assert (info.config.max_age_secs, info.config.max_msgs, info.config.discard) == (60, 3, "new")
    assert info.config.retention == "work_queue"
    q = await c.query("SELECT COUNT(*) AS n FROM s")
    assert (q.columns, q.rows, q.row_count, q.execution_time_ms, q.truncated) == (["n"], [[3]], 1, 2, False)


async def test_fails_pending_requests_with_connection_error_when_the_connection_drops(
    server: FakeServer, connect: Connect
) -> None:
    c = await connect()
    server.handler = lambda conn, corr, req: True
    p = asyncio.ensure_future(c.metadata())
    await server.until(lambda: len(server.last.of(m.Metadata)) == 1)
    server.last.destroy()
    with pytest.raises(ConnectionError):
        await p
    with pytest.raises(ConnectionError):
        await c.ping()


async def test_seek_targets(server: FakeServer, connect: Connect) -> None:
    from datetime import datetime, timezone

    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.SeekConsumer):
            conn.reply(corr, m.Ok())

    server.handler = handler
    await c.seek("c", "earliest")
    await c.seek("c", "latest")
    await c.seek("c", 42)
    await c.seek("c", datetime.fromtimestamp(1.5, tz=timezone.utc))
    await c.seek("c", exspeed.SeekTime(9))
    with pytest.raises(exspeed.ExspeedError):
        await c.seek("c", "middle")  # type: ignore[arg-type]
    assert [(r.kind, r.value) for _, r in server.last.of(m.SeekConsumer)] == [
        (0, 0),
        (1, 0),
        (2, 42),
        (3, 1500),
        (3, 9),
    ]


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


async def test_keeps_deliver_frames_that_arrive_in_the_same_chunk_as_subscribe_ok(
    server: FakeServer, connect: Connect
) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if not isinstance(req, m.Subscribe):
            return None
        conn.reply_many([(corr, m.SubscribeOk(5)), (0, m.Deliver(5, [rec(0), rec(1)]))])
        return True

    server.handler = handler
    sub = await c.subscribe("billing", window=10)
    assert sub.id == 5
    m0 = await sub.next(timeout=0.5)
    m1 = await sub.next(timeout=0.5)
    assert m0 is not None and m1 is not None
    assert [m0.offset, m1.offset] == [0, 1]
    assert m0.text() == "v0"
    assert m0.header("h") == "1"
    assert m0.header("missing") is None
    assert m0.delivery_count == 1
    assert m0.consumer == "billing"
    assert m0.timestamp_ms == 1_700_000_000_000
    assert m0.timestamp.year == 2023


async def test_returns_credit_in_batches_of_half_the_window(server: FakeServer, connect: Connect) -> None:
    c = await connect()
    sub_corr = 0

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal sub_corr
        if isinstance(req, m.Subscribe):
            sub_corr = corr
            conn.reply(corr, m.SubscribeOk(9))

    server.handler = handler
    sub = await c.subscribe("c", window=4)
    assert server.last.of(m.Subscribe)[0][1].credits == 4
    assert sub_corr != 0
    server.last.reply(0, m.Deliver(9, [rec(i) for i in range(4)]))
    await sub.next()
    assert server.last.of(m.Credit) == []
    await sub.next()
    await server.until(lambda: len(server.last.of(m.Credit)) == 1)
    assert server.last.of(m.Credit)[0] == (0, m.Credit(9, 2))
    await sub.next()
    await sub.next()
    await server.until(lambda: len(server.last.of(m.Credit)) == 2)


async def test_acks_fire_and_forget_and_settles_with_nack_term_in_progress(
    server: FakeServer, connect: Connect
) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply_many([(corr, m.SubscribeOk(1)), (0, m.Deliver(1, [rec(7)]))])
        elif isinstance(req, (m.Nack, m.Term, m.InProgress)):
            conn.reply(corr, m.Ok())

    server.handler = handler
    sub = await c.subscribe("c")
    msg = await sub.next()
    assert msg is not None
    msg.ack()
    await msg.nack(0.25)
    await msg.term("poison")
    await msg.in_progress()
    await msg.nack()
    got = [(r.corr == 0, r.req) for r in server.last.received[2:]]
    assert got == [
        (True, m.Ack("c", [7])),
        (False, m.Nack("c", 7, 250)),
        (False, m.Term("c", 7, "poison")),
        (False, m.InProgress("c", [7])),
        (False, m.Nack("c", 7, 0)),
    ]


async def test_sends_acks_made_in_the_same_tick_as_one_frame_before_any_later_request(
    server: FakeServer, connect: Connect
) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply_many([(corr, m.SubscribeOk(1)), (0, m.Deliver(1, [rec(1), rec(2), rec(3)]))])

    server.handler = handler
    sub = await c.subscribe("c")
    msgs: list[Message] = []
    for _ in range(3):
        x = await sub.next()
        assert x is not None
        msgs.append(x)
    for x in msgs:
        x.ack()
    ping = asyncio.ensure_future(c.ping())
    await server.until(lambda: len(server.last.of(m.Ping)) == 1)
    await ping
    # (window 256: no Credit is due after three messages)
    assert server.last.types()[2:] == ["Ack", "Ping"]
    assert server.last.of(m.Ack) == [(0, m.Ack("c", [1, 2, 3]))]


async def test_flushes_queued_acks_on_close(server: FakeServer) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply_many([(corr, m.SubscribeOk(1)), (0, m.Deliver(1, [rec(4)]))])

    server.handler = handler
    c = await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=False)
    sub = await c.subscribe("c")
    msg = await sub.next()
    assert msg is not None
    msg.ack()
    conn = server.last
    await c.close()
    await server.until(lambda: len(conn.of(m.Ack)) == 1)


async def test_confirmed_ack(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Ack):
            conn.reply(corr, m.Ok())

    server.handler = handler
    await c.ack("c", [1, 2])
    assert server.last.of(m.Ack)[0][0] != 0


async def test_reports_failed_fire_and_forget_requests_as_error_events(server: FakeServer, connect: Connect) -> None:
    c = await connect()
    errors: list[ServerError] = []
    c.on("error", errors.append)
    server.last.reply(0, m.Error(404, "consumer 'x' not found", None))
    await server.until(lambda: len(errors) == 1)
    assert errors[0].code == 404


async def test_async_event_listeners_run_as_tasks(server: FakeServer, connect: Connect) -> None:
    c = await connect()
    got: list[int] = []

    async def on_error(e: ServerError) -> None:
        await asyncio.sleep(0)
        got.append(e.code)

    c.on("error", on_error)
    server.last.reply(0, m.Error(429, "slow down", None))
    await server.until(lambda: got == [429])
    c.off("error", on_error)


async def test_ends_on_subscription_ended_after_yielding_buffered_records(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply_many(
                [
                    (corr, m.SubscribeOk(3)),
                    (0, m.Deliver(3, [rec(0)])),
                    (0, m.SubscriptionEnded(3, 404, "consumer deleted")),
                ]
            )

    server.handler = handler
    sub = await c.subscribe("c")
    seen = [x.offset async for x in sub]
    assert seen == [0]
    assert sub.end_reason == exspeed.EndReason(404, "consumer deleted")


async def test_unsubscribes_when_an_async_with_block_exits(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply_many([(corr, m.SubscribeOk(4)), (0, m.Deliver(4, [rec(0), rec(1)]))])
        elif isinstance(req, m.Unsubscribe):
            conn.reply(corr, m.Ok())

    server.handler = handler
    async with c.subscribe("c") as sub:
        async for _ in sub:
            break
    assert server.last.of(m.Unsubscribe)[0][1] == m.Unsubscribe(4)
    assert sub.end_reason == exspeed.EndReason(0, "unsubscribed")
    assert await sub.next() is None


async def test_next_with_timeout_returns_none(server: FakeServer, connect: Connect) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply(corr, m.SubscribeOk(4))

    server.handler = handler
    sub = await c.subscribe("c")
    assert await sub.next(timeout=0.05) is None
    # A record that arrives later is still delivered to the next call.
    server.last.reply(0, m.Deliver(4, [rec(9)]))
    got = await sub.next(timeout=1)
    assert got is not None and got.offset == 9


async def test_ends_subscriptions_with_503_when_the_connection_is_lost_and_reconnect_is_off(
    server: FakeServer, connect: Connect
) -> None:
    c = await connect()

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Subscribe):
            conn.reply(corr, m.SubscribeOk(1))

    server.handler = handler
    sub = await c.subscribe("c")
    closed: list[object] = []
    c.on("close", closed.append)
    nxt = asyncio.ensure_future(sub.next())
    await asyncio.sleep(0.01)
    server.last.destroy()
    assert await nxt is None
    await server.until(lambda: len(closed) == 1)
    assert isinstance(closed[0], ConnectionError)
    assert sub.end_reason is not None and sub.end_reason.code == 503
    assert not c.connected


# ---------------------------------------------------------------------------
# Reconnection
# ---------------------------------------------------------------------------


async def test_reconnects_recreates_ephemeral_consumers_and_resubscribes(server: FakeServer) -> None:
    next_sub = 1

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal next_sub
        if isinstance(req, m.Subscribe):
            conn.reply(corr, m.SubscribeOk(next_sub))
            next_sub += 1
        if isinstance(req, m.CreateConsumer):
            conn.reply(corr, m.Json(b"{}"))

    server.handler = handler
    c = await ExspeedClient.connect(
        port=server.port, keepalive=0, reconnect=ReconnectOptions(initial_delay=0.01, max_delay=0.02)
    )
    try:
        await c.create_consumer(exspeed.ConsumerSpec("tmp", "s", ephemeral=True))
        sub = await c.subscribe("tmp", window=8)
        assert sub.id == 1
        server.last.reply(0, m.Deliver(1, [rec(0)]))
        first = await sub.next()
        assert first is not None and first.offset == 0

        events: list[str] = []
        reconnected = asyncio.get_running_loop().create_future()
        c.on("disconnect", lambda e: events.append("disconnect"))
        c.on("reconnect", lambda info: reconnected.done() or reconnected.set_result(info))
        server.conns[0].destroy()
        info = await asyncio.wait_for(reconnected, 5)
        events.append("reconnect")
        assert info == exspeed.ServerInfo("test", "n1", None)
        assert events == ["disconnect", "reconnect"]
        assert len(server.conns) == 2
        second = server.conns[1]
        assert second.types() == ["Connect", "CreateConsumer", "Subscribe"]
        assert second.of(m.Subscribe)[0][1] == m.Subscribe("tmp", 8)
        assert sub.id == 2

        second.reply(0, m.Deliver(2, [rec(1)]))
        again = await sub.next(timeout=1)
        assert again is not None and again.offset == 1
        assert c.connected
    finally:
        await c.close()


async def test_fails_requests_while_reconnecting(server: FakeServer) -> None:
    c = await ExspeedClient.connect(
        port=server.port, keepalive=0, reconnect=ReconnectOptions(initial_delay=0.2, max_delay=0.2)
    )
    try:
        disconnected = asyncio.get_running_loop().create_future()
        c.on("disconnect", lambda e: disconnected.done() or disconnected.set_result(e))
        server.last.destroy()
        await asyncio.wait_for(disconnected, 2)
        with pytest.raises(ConnectionError, match="reconnecting"):
            await c.ping()
    finally:
        await c.close()


async def test_ends_a_subscription_whose_resubscribe_fails(server: FakeServer) -> None:
    first = True

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal first
        if not isinstance(req, m.Subscribe):
            return
        if first:
            conn.reply(corr, m.SubscribeOk(1))
        else:
            conn.reply(corr, m.Error(404, "consumer 'c' not found", None))
        first = False

    server.handler = handler
    c = await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=ReconnectOptions(initial_delay=0.01))
    try:
        sub = await c.subscribe("c")
        reconnected = asyncio.get_running_loop().create_future()
        c.on("reconnect", lambda info: reconnected.done() or reconnected.set_result(info))
        server.conns[0].destroy()
        await asyncio.wait_for(reconnected, 5)
        assert await sub.next(timeout=1) is None
        assert sub.end_reason == exspeed.EndReason(404, "consumer 'c' not found")
    finally:
        await c.close()


async def test_gives_up_after_max_attempts_and_closes(server: FakeServer) -> None:
    c = await ExspeedClient.connect(
        port=server.port, keepalive=0, reconnect=ReconnectOptions(max_attempts=2, initial_delay=0.01)
    )
    closed = asyncio.get_running_loop().create_future()
    c.on("close", lambda e: closed.done() or closed.set_result(e))
    await server.close()
    err = await asyncio.wait_for(closed, 5)
    assert isinstance(err, ConnectionError)
    assert not c.connected
    with pytest.raises(ConnectionError, match="closed"):
        await c.ping()


async def test_stops_reconnecting_when_the_credential_is_rejected(server: FakeServer) -> None:
    c = await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=ReconnectOptions(initial_delay=0.01))

    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Connect):
            conn.reply(corr, m.Error(401, "unauthorized", None))
            return True
        return None

    server.handler = handler
    closed = asyncio.get_running_loop().create_future()
    c.on("close", lambda e: closed.done() or closed.set_result(e))
    server.last.destroy()
    err = await asyncio.wait_for(closed, 5)
    assert isinstance(err, ServerError) and err.code == 401
    assert len(server.conns) == 2  # one retry, then it gave up


# ---------------------------------------------------------------------------
# Keepalive and cluster discovery
# ---------------------------------------------------------------------------


async def test_pings_on_the_configured_interval(server: FakeServer, connect: Connect) -> None:
    await connect(keepalive=0.03)
    await server.until(lambda: len(server.last.of(m.Ping)) >= 2, timeout=1)


async def test_a_ping_that_times_out_drops_the_connection(server: FakeServer, connect: Connect) -> None:
    c = await connect(keepalive=0.03, request_timeout=0.05)
    server.handler = lambda conn, corr, req: isinstance(req, m.Ping)  # never answer pings
    closed: list[object] = []
    c.on("close", closed.append)
    await server.until(lambda: len(closed) == 1, timeout=2)
    assert not c.connected


async def test_connects_to_the_leader_by_following_hints_from_a_seed() -> None:
    leader_port = 0

    def metadata(conn: FakeConn, corr: int, is_leader: bool, leader: str | None) -> None:
        body = {"node_id": "x", "is_leader": is_leader, "leader": leader, "server_version": "t"}
        conn.reply(corr, m.Json(json.dumps(body).encode()))

    def follower_handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Connect):
            conn.reply(corr, m.ConnectOk("t", "f", f"127.0.0.1:{leader_port}"))
            return True
        if isinstance(req, m.Metadata):
            metadata(conn, corr, False, f"127.0.0.1:{leader_port}")
            return True
        return None

    def leader_handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Connect):
            conn.reply(corr, m.ConnectOk("t", "l", None))
            return True
        if isinstance(req, m.Metadata):
            metadata(conn, corr, True, None)
            return True
        return None

    follower = await FakeServer.start(follower_handler)
    leader = await FakeServer.start(leader_handler)
    leader_port = leader.port
    try:
        c = await ExspeedClient.connect(
            servers=[f"127.0.0.1:{follower.port}"],
            keepalive=0,
            reconnect=ReconnectOptions(initial_delay=0.01, max_delay=0.02, max_attempts=20),
        )
        assert c.server_info.node_id == "l"
        await c.close()
        # Without seeds, a handshake naming another leader is followed too.
        c = await ExspeedClient.connect(port=follower.port, keepalive=0, reconnect=False)
        assert c.server_info.node_id == "l"
        await c.close()
    finally:
        await leader.close()
        await follower.close()


# ---------------------------------------------------------------------------
# Protocol checks
# ---------------------------------------------------------------------------


async def test_refuses_a_server_speaking_another_protocol_version(server: FakeServer) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Connect):
            frame = bytearray(m.response_frame(m.ConnectOk("old", "n1", None), corr))
            frame[0] = 1  # protocol version 1
            assert conn.transport is not None
            conn.transport.write(bytes(frame))
            return True
        return None

    server.handler = handler
    with pytest.raises(ConnectionError, match="unsupported protocol version"):
        await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=False)


async def test_verify_crc_rejects_a_corrupted_record(server: FakeServer, connect: Connect) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.Read):
            payload = bytearray(m.encode_response(m.ReadResult(1, 1, [rec(0)])))
            payload[-1] ^= 0x01  # flip a bit in the last header value: the CRC no longer matches
            assert conn.transport is not None
            conn.transport.write(encode_frame(OpCode.READ_RESULT, corr, bytes(payload)))
            return True
        return None

    server.handler = handler
    plain = await connect()
    assert len((await plain.read("s")).records) == 1  # not verified by default
    checked = await connect(verify_crc=True)
    with pytest.raises(exspeed.ProtocolError, match="CRC"):
        await checked.read("s")
