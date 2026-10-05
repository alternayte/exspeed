"""Core pub/sub, request-reply, KV buckets and consumer info against the
scriptable fake server."""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fake_server import FakeConn, FakeServer

import exspeed
from exspeed import (
    ConnectionError,
    ConsumerSpec,
    ExspeedClient,
    ExspeedError,
    ReconnectOptions,
    ServerError,
    TimeoutError,
)
from exspeed.protocol import WireRecord
from exspeed.protocol import messages as m


@pytest.fixture
async def client(server: FakeServer) -> AsyncIterator[ExspeedClient]:
    c = await ExspeedClient.connect(port=server.port, keepalive=0, reconnect=False)
    yield c
    await c.close()


def ok(conn: FakeConn, corr: int) -> None:
    conn.reply(corr, m.Ok())


def kv_rec(offset: int, key: str, value: str, op: str | None = None) -> WireRecord:
    """A KV record as the server stores it: subject = key, raw stream offset."""
    return WireRecord(
        offset=offset,
        timestamp_ns=1_700_000_000_000_000_000 + offset,
        delivery_count=0,
        subject=key,
        key=None,
        value=value.encode(),
        headers=[("exspeed-kv-op", op)] if op else [],
    )


def core_msg(sub_id: int, subject: str, value: bytes, reply_to: str | None = None) -> m.CoreMsg:
    return m.CoreMsg(sub_id, subject, reply_to, [], value)


# ---------------------------------------------------------------------------
# Core pub/sub
# ---------------------------------------------------------------------------


async def test_publishes_a_core_message_and_waits_for_ok(server: FakeServer, client: ExspeedClient) -> None:
    server.handler = lambda conn, corr, req: ok(conn, corr) if isinstance(req, m.CorePublish) else None
    await client.publish_core("orders.created", {"id": 1}, headers={"trace-id": "t"})
    [(corr, p)] = server.last.of(m.CorePublish)
    assert corr != 0
    assert (p.subject, p.reply_to, p.headers, p.value) == ("orders.created", None, [("trace-id", "t")], b'{"id":1}')


async def test_subscribes_with_a_queue_group_responds_and_unsubscribes(
    server: FakeServer, client: ExspeedClient
) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.CoreSubscribe):
            conn.reply_many(
                [
                    (corr, m.SubscribeOk(0x80000001)),
                    (0, m.CoreMsg(0x80000001, "svc.echo", "_INBOX.x.1", [("h", "1")], b'{"q":1}')),
                    (0, core_msg(0x80000099, "other", b"ignored")),
                ]
            )
        elif isinstance(req, (m.CorePublish, m.Unsubscribe)):
            ok(conn, corr)

    server.handler = handler
    sub = await client.subscribe_core("svc.*", queue="workers")
    assert server.last.of(m.CoreSubscribe)[0][1] == m.CoreSubscribe("svc.*", "workers")
    assert sub.id == 0x80000001
    msg = await sub.next(timeout=1)
    assert msg is not None
    assert (msg.subject, msg.reply_to, msg.header("h"), msg.json()) == ("svc.echo", "_INBOX.x.1", "1", {"q": 1})
    await msg.respond("pong")
    resp = server.last.of(m.CorePublish)[0][1]
    assert (resp.subject, resp.reply_to, resp.value) == ("_INBOX.x.1", None, b"pong")
    assert await sub.next(timeout=0.1) is None  # the other sub's message isn't routed here

    await sub.unsubscribe()
    assert server.last.of(m.Unsubscribe)[0][1] == m.Unsubscribe(0x80000001)
    assert sub.end_reason == exspeed.EndReason(0, "unsubscribed")


async def test_refuses_to_respond_to_a_message_without_reply_to(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.CoreSubscribe):
            conn.reply_many([(corr, m.SubscribeOk(0x80000001)), (0, core_msg(0x80000001, "a", b"x"))])

    server.handler = handler
    sub = await client.subscribe_core("a")
    msg = await sub.next()
    assert msg is not None
    with pytest.raises(ExspeedError):
        await msg.respond("no")


async def test_core_sub_ends_when_the_server_ends_it_after_yielding_buffered(
    server: FakeServer, client: ExspeedClient
) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.CoreSubscribe):
            conn.reply_many(
                [
                    (corr, m.SubscribeOk(0x80000002)),
                    (0, core_msg(0x80000002, "a", b"1")),
                    (0, m.SubscriptionEnded(0x80000002, 503, "leadership moved")),
                ]
            )

    server.handler = handler
    sub = await client.subscribe_core("a")
    seen = [x.text() async for x in sub]
    assert seen == ["1"]
    assert sub.end_reason == exspeed.EndReason(503, "leadership moved")


async def test_core_sub_unsubscribes_when_an_async_with_block_exits(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.CoreSubscribe):
            conn.reply_many([(corr, m.SubscribeOk(0x80000003)), (0, core_msg(0x80000003, "a", b"1"))])
        elif isinstance(req, m.Unsubscribe):
            ok(conn, corr)

    server.handler = handler
    async with client.subscribe_core("a") as sub:
        async for _ in sub:
            break
    assert server.last.of(m.Unsubscribe)[0][1] == m.Unsubscribe(0x80000003)
    assert sub.closed


# ---------------------------------------------------------------------------
# Request-reply
# ---------------------------------------------------------------------------


class InboxState:
    def __init__(self) -> None:
        self.subs: list[str] = []


def inbox_server(server: FakeServer) -> InboxState:
    """Answers CoreSubscribe for the inbox and records requests; replies are sent by the test."""
    state = InboxState()
    next_id = 0x80000010

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal next_id
        if isinstance(req, m.CoreSubscribe):
            state.subs.append(req.subject)
            conn.reply(corr, m.SubscribeOk(next_id))
            next_id += 1
        elif isinstance(req, m.CorePublish):
            ok(conn, corr)

    server.handler = handler
    return state


async def test_shares_one_inbox_and_routes_responses_by_the_last_subject_token(
    server: FakeServer, client: ExspeedClient
) -> None:
    state = inbox_server(server)
    a = asyncio.ensure_future(client.request("svc.a", "1"))
    b = asyncio.ensure_future(client.request("svc.b", {"n": 2}, headers={"h": "v"}))
    await server.until(lambda: len(server.last.of(m.CorePublish)) == 2)
    assert len(state.subs) == 1
    assert re.fullmatch(r"_INBOX\.[0-9a-f]+\.\*", state.subs[0])
    prefix = state.subs[0][:-2]
    (_, pa), (_, pb) = server.last.of(m.CorePublish)
    assert pa.reply_to == f"{prefix}.1"
    assert pb.reply_to == f"{prefix}.2"
    assert pb.headers == [("h", "v")]
    assert pb.value == b'{"n":2}'
    # Out of order, on the inbox subscription.
    server.last.reply(0, core_msg(0x80000010, pb.reply_to, b"B"))
    server.last.reply(0, core_msg(0x80000010, pa.reply_to, b"A"))
    assert (await a).text() == "A"
    assert (await b).text() == "B"

    third = asyncio.ensure_future(client.request("svc.a", "3"))
    await server.until(lambda: len(server.last.of(m.CorePublish)) == 3)
    assert len(state.subs) == 1  # still the same inbox
    p3 = server.last.of(m.CorePublish)[2][1]
    server.last.reply(0, core_msg(0x80000010, p3.reply_to, b"C"))
    assert (await third).text() == "C"


async def test_request_rejects_at_once_with_404_when_there_are_no_responders(
    server: FakeServer, client: ExspeedClient
) -> None:
    inbox_server(server)
    prev = server.handler

    def handler(conn: FakeConn, corr: int, req: Any) -> bool | None:
        if isinstance(req, m.CorePublish):
            conn.reply(corr, m.Error(404, "no responders for 'svc.none'", None))
            return True
        return prev(conn, corr, req)

    server.handler = handler
    t = time.monotonic()
    with pytest.raises(ServerError) as e:
        await client.request("svc.none", "x", timeout=5)
    assert e.value.code == 404
    assert time.monotonic() - t < 1


async def test_request_times_out_and_ignores_a_late_response(server: FakeServer, client: ExspeedClient) -> None:
    inbox_server(server)
    with pytest.raises(TimeoutError):
        await client.request("svc.slow", "x", timeout=0.1)
    p = server.last.of(m.CorePublish)[0][1]
    server.last.reply(0, core_msg(0x80000010, p.reply_to, b"late"))
    await client.ping()  # the late response was dropped without trouble


async def test_fails_waiting_requests_when_the_inbox_ends_and_subscribes_a_new_one(
    server: FakeServer, client: ExspeedClient
) -> None:
    state = inbox_server(server)
    pending = asyncio.ensure_future(client.request("svc.a", "x", timeout=5))
    await server.until(lambda: len(server.last.of(m.CorePublish)) == 1)
    server.last.reply(0, m.SubscriptionEnded(0x80000010, 503, "leadership moved"))
    with pytest.raises(ServerError) as e:
        await pending
    assert e.value.code == 503

    again = asyncio.ensure_future(client.request("svc.a", "y"))
    await server.until(lambda: len(server.last.of(m.CorePublish)) == 2)
    assert len(state.subs) == 2
    assert state.subs[1] != state.subs[0]
    p = server.last.of(m.CorePublish)[1][1]
    assert p.reply_to.startswith(state.subs[1][:-2])
    server.last.reply(0, core_msg(0x80000011, p.reply_to, b"ok"))
    assert (await again).text() == "ok"


async def test_after_a_reconnect_resubscribes_core_subs_and_sets_up_a_new_inbox(server: FakeServer) -> None:
    next_id = 0x80000020

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal next_id
        if isinstance(req, m.CoreSubscribe):
            conn.reply(corr, m.SubscribeOk(next_id))
            next_id += 1
        elif isinstance(req, m.CorePublish):
            ok(conn, corr)

    server.handler = handler
    c = await ExspeedClient.connect(
        port=server.port, keepalive=0, reconnect=ReconnectOptions(initial_delay=0.01, max_delay=0.02)
    )
    try:
        sub = await c.subscribe_core("events.>", queue="g")
        pending = asyncio.ensure_future(c.request("svc.a", "x", timeout=5))
        await server.until(lambda: len(server.last.of(m.CorePublish)) == 1)
        first_inbox = server.last.of(m.CoreSubscribe)[1][1].subject

        reconnected = asyncio.get_running_loop().create_future()
        c.on("reconnect", lambda info: reconnected.done() or reconnected.set_result(info))
        server.conns[0].destroy()
        with pytest.raises(ConnectionError):
            await pending
        await asyncio.wait_for(reconnected, 5)
        second = server.conns[1]
        assert [r for _, r in second.of(m.CoreSubscribe)] == [m.CoreSubscribe("events.>", "g")]
        second.reply(0, core_msg(sub.id, "events.x", b"after"))
        got = await sub.next(timeout=1)
        assert got is not None and got.text() == "after"

        again = asyncio.ensure_future(c.request("svc.a", "y"))
        await server.until(lambda: len(second.of(m.CorePublish)) == 1)
        inbox = second.of(m.CoreSubscribe)[1][1].subject
        assert inbox != first_inbox
        p = second.of(m.CorePublish)[0][1]
        second.reply(0, core_msg(next_id - 1, p.reply_to, b"ok"))
        assert (await again).text() == "ok"
    finally:
        await c.close()


# ---------------------------------------------------------------------------
# KV buckets
# ---------------------------------------------------------------------------


async def test_kv_encodes_create_put_create_key_update_delete_purge(server: FakeServer, client: ExspeedClient) -> None:
    rev = 0

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        nonlocal rev
        if isinstance(req, m.KvCreateBucket):
            ok(conn, corr)
        if isinstance(req, (m.KvPut, m.KvDelete)):
            rev += 1
            conn.reply(corr, m.PublishOk(rev, False))

    server.handler = handler
    kv = client.kv("cfg")
    assert kv.stream == "KV_cfg"
    await kv.create(history=5, ttl=60)
    await client.kv("plain").create()
    assert await kv.put("a", {"on": True}, ttl=0.5) == 1
    assert await kv.create_key("b", "x") == 2
    assert await kv.update("b", "y", 2) == 3
    assert await kv.delete("a") == 4
    assert await kv.purge("b", expected_revision=3) == 5

    reqs = [r.req for r in server.last.received if type(r.req).__name__.startswith("Kv")]
    assert reqs == [
        m.KvCreateBucket("cfg", 5, 60_000, 0),
        m.KvCreateBucket("plain", 0, 0, 0),
        m.KvPut("cfg", "a", b'{"on":true}', None, 500),
        m.KvPut("cfg", "b", b"x", 0, None),
        m.KvPut("cfg", "b", b"y", 2, None),
        m.KvDelete("cfg", "a", False, None),
        m.KvDelete("cfg", "b", True, 3),
    ]


async def test_kv_passes_a_cas_conflict_through_as_409(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.KvPut):
            conn.reply(corr, m.Error(409, "wrong revision", b'{"current_revision":7}'))

    server.handler = handler
    with pytest.raises(ServerError) as e:
        await client.kv("b").update("k", "v", 3)
    assert (e.value.code, e.value.detail) == (409, {"current_revision": 7})


async def test_kv_turns_records_into_entries_and_404_key_into_none(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if not isinstance(req, m.KvGet):
            return
        if req.key == "live":
            conn.reply(corr, m.Messages([kv_rec(4, "live", '{"n":1}')]))
        elif req.key == "gone":
            conn.reply(corr, m.Error(404, "key 'gone' not found", None))
        elif req.key == "old":
            conn.reply(corr, m.Messages([kv_rec(req.revision - 1, "old", "v1")]))
        else:
            conn.reply(corr, m.Messages([]))

    server.handler = handler
    kv = client.kv("b")
    e = await kv.get("live")
    assert e is not None
    assert (e.key, e.revision, e.op, e.json()) == ("live", 5, "put", {"n": 1})
    assert e.timestamp_ns == 1_700_000_000_000_000_004
    assert e.timestamp_ms == 1_700_000_000_000
    assert await kv.get("gone") is None
    assert await kv.get("empty") is None
    old = await kv.get_revision("old", 2)
    assert old is not None and (old.revision, old.text()) == (2, "v1")
    assert [r.revision for _, r in server.last.of(m.KvGet)] == [None, None, None, 2]


async def test_kv_raises_when_the_bucket_doesnt_exist(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.KvGet):
            conn.reply(corr, m.Error(404, "bucket 'nope' not found", None))

    server.handler = handler
    with pytest.raises(ServerError) as e:
        await client.kv("nope").get("k")
    assert e.value.code == 404


async def test_kv_lists_keys_and_history_tombstones_included(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.KvKeys):
            conn.reply(corr, m.Json(b'["a.1","a.2"]'))
        if isinstance(req, m.KvHistory):
            conn.reply(
                corr,
                m.Messages(
                    [kv_rec(0, "k", "v1"), kv_rec(3, "k", "", "DEL"), kv_rec(5, "k", "v2"), kv_rec(6, "k", "", "PURGE")]
                ),
            )
        if isinstance(req, m.DeleteStream):
            ok(conn, corr)

    server.handler = handler
    kv = client.kv("b")
    assert await kv.keys("a.*") == ["a.1", "a.2"]
    assert await kv.keys() == ["a.1", "a.2"]
    assert [r.filter for _, r in server.last.of(m.KvKeys)] == ["a.*", ""]
    h = await kv.history("k")
    assert [(e.revision, e.op) for e in h] == [(1, "put"), (4, "delete"), (6, "put"), (7, "purge")]
    await kv.destroy()
    assert server.last.of(m.DeleteStream)[0][1] == m.DeleteStream("KV_b")


async def test_kv_watch_live_keys_first_sorted_by_revision_then_every_change(
    server: FakeServer, client: ExspeedClient
) -> None:
    reads: list[m.Read] = []

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if not isinstance(req, m.Read):
            return
        reads.append(req)
        if req.wait_ms == 0 and req.from_offset == 0:
            # Snapshot, page 1 of 2 (high watermark 5).
            conn.reply(corr, m.ReadResult(3, 5, [kv_rec(0, "a", "a1"), kv_rec(1, "b", "b1"), kv_rec(2, "a", "a2")]))
        elif req.wait_ms == 0 and req.from_offset == 3:
            conn.reply(corr, m.ReadResult(5, 6, [kv_rec(3, "c", "c1"), kv_rec(4, "b", "", "DEL")]))
        else:
            conn.reply(corr, m.ReadResult(7, 7, [kv_rec(5, "a", "a3"), kv_rec(6, "c", "", "DEL")]))

    server.handler = handler
    w = client.kv("b").watch("x.>")
    seen: list[tuple[str, int, str]] = []
    async with w:
        async for e in w:
            seen.append((e.key, e.revision, e.op))
            if len(seen) == 4:
                break
    # b was deleted before the snapshot ended: left out. a (rev 3) before c (rev 4).
    assert seen == [("a", 3, "put"), ("c", 4, "put"), ("a", 6, "put"), ("c", 7, "delete")]
    assert [(r.stream, r.from_offset, r.wait_ms, r.filter) for r in reads] == [
        ("KV_b", 0, 0, "x.>"),
        ("KV_b", 3, 0, "x.>"),
        ("KV_b", 5, 10_000, "x.>"),
    ]
    assert w.closed
    assert await w.next() is None


async def test_kv_watch_next_with_timeout_keeps_what_a_pending_long_poll_brings(
    server: FakeServer, client: ExspeedClient
) -> None:
    held: list[tuple[FakeConn, int]] = []

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if not isinstance(req, m.Read):
            return
        if req.wait_ms == 0:
            conn.reply(corr, m.ReadResult(0, 0, []))
        else:
            held.append((conn, corr))

    server.handler = handler
    w = client.kv("b").watch()
    assert await w.next(timeout=0.1) is None
    await server.until(lambda: len(held) == 1)
    conn, corr = held[0]
    conn.reply(corr, m.ReadResult(1, 1, [kv_rec(0, "k", "v")]))
    e = await w.next(timeout=1)
    assert e is not None and (e.key, e.revision) == ("k", 1)
    assert len(server.last.of(m.Read)) == 2  # the timed-out next() didn't start another read
    w.stop()


async def test_kv_watch_raises_a_failed_read(server: FakeServer, client: ExspeedClient) -> None:
    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, m.Read):
            conn.reply(corr, m.Error(404, "stream 'KV_b' not found", None))

    server.handler = handler
    w = client.kv("b").watch()
    with pytest.raises(ServerError):
        await w.next(timeout=1)


# ---------------------------------------------------------------------------
# Consumer info
# ---------------------------------------------------------------------------


async def test_consumer_info_keeps_filter_headers_and_reports_num_delayed(
    server: FakeServer, client: ExspeedClient
) -> None:
    info = {
        "spec": {
            "name": "c",
            "stream": "s",
            "filter_headers": {"x_tenant_id": "acme"},
            "header_match": "any",
            "single_active": True,
            "priority_window": 5,
            "dead_letter_expired": True,
            "deliver": {"from_offset": 3},
            "dlq_stream": "dlq",
        },
        "num_delayed": 2,
        "ack_floor": 1,
        "stats": {"delivered": 4, "dead_lettered": 1},
    }

    def handler(conn: FakeConn, corr: int, req: Any) -> None:
        if isinstance(req, (m.ConsumerInfo, m.CreateConsumer)):
            conn.reply(corr, m.Json(json.dumps(info).encode()))
        if isinstance(req, m.ListConsumers):
            conn.reply(corr, m.Json(json.dumps([info, {"spec": {"name": "d", "stream": "s"}}]).encode()))
        if isinstance(req, m.DeleteConsumer):
            ok(conn, corr)

    server.handler = handler
    i = await client.consumer_info("c")
    assert i.spec.filter_headers == {"x_tenant_id": "acme"}
    assert (i.spec.header_match, i.spec.single_active, i.spec.priority_window) == ("any", True, 5)
    assert i.spec.dead_letter_expired is True
    assert i.spec.deliver == exspeed.DeliverFromOffset(3)
    assert i.spec.dlq_stream == "dlq"
    assert (i.num_delayed, i.ack_floor) == (2, 1)
    assert (i.stats.delivered, i.stats.dead_lettered) == (4, 1)
    created = await client.create_consumer(
        ConsumerSpec("c", "s", filter_headers={"x_tenant_id": "acme"}, header_match="any")
    )
    assert created.spec.filter_headers == {"x_tenant_id": "acme"}
    sent = server.last.of(m.CreateConsumer)[0][1].spec
    assert (sent["filter_headers"], sent["header_match"]) == ({"x_tenant_id": "acme"}, "any")
    listed = await client.list_consumers()
    assert [x.spec.filter_headers for x in listed] == [{"x_tenant_id": "acme"}, {}]
    assert listed[1].spec.ack_wait_ms == 30_000  # server defaults filled in
    assert server.last.of(m.ListConsumers)[0][1] == m.ListConsumers(None)
    await client.list_consumers("s")
    assert server.last.of(m.ListConsumers)[1][1] == m.ListConsumers("s")
    await client.delete_consumer("c")
