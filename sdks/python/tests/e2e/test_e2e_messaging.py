"""End-to-end tests of stream limits, time headers, routing settings, core
pub/sub, request-reply and KV buckets against a real exspeed server."""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from collections.abc import AsyncIterator

import pytest
from harness import ExspeedServer, eventually, uniq

from exspeed import (
    ConsumerSpec,
    CoreMessage,
    CoreSubscription,
    ExspeedClient,
    KvEntry,
    KvWatch,
    ServerError,
    StreamSpec,
    TimeoutError,
)


@pytest.fixture
async def pair(exspeed_server: ExspeedServer) -> AsyncIterator[tuple[ExspeedClient, ExspeedClient]]:
    a = await exspeed_server.connect()
    b = await exspeed_server.connect()
    try:
        yield a, b
    finally:
        await a.close()
        await b.close()


# ---------------------------------------------------------------------------
# Stream limits and time headers
# ---------------------------------------------------------------------------


async def test_hides_records_whose_ttl_has_passed(client: ExspeedClient) -> None:
    s = uniq("ttl")
    await client.create_stream(StreamSpec(s, allow_msg_ttl=True))
    assert (await client.stream_info(s)).config.allow_msg_ttl is True

    await client.publish(s, "jobs.a", "short", ttl=0.15)
    await client.publish(s, "jobs.a", "keep")
    await client.publish(s, "jobs.a", "long", ttl="1h")
    c = uniq("ttl-c")
    await client.create_consumer(ConsumerSpec(c, s))
    await asyncio.sleep(0.4)

    assert [r.text() for r in (await client.read(s)).records] == ["keep", "long"]
    got = await client.pull(c, max_messages=10, expires=0.5)
    assert [x.text() for x in got] == ["keep", "long"]
    assert got[1].header("exspeed-ttl") == "1h"


async def test_rejects_time_headers_on_a_stream_that_doesnt_allow_them(client: ExspeedClient) -> None:
    s = uniq("plain")
    await client.create_stream(s)
    with pytest.raises(ServerError) as e:
        await client.publish(s, "a.b", "x", ttl=1)
    assert e.value.code == 400
    with pytest.raises(ServerError) as e:
        await client.publish(s, "a.b", "x", delay=1)
    assert e.value.code == 400


async def test_delivers_delayed_records_to_consumers_when_due(client: ExspeedClient) -> None:
    s = uniq("delay")
    await client.create_stream(StreamSpec(s, allow_delayed=True))
    c = uniq("delay-c")
    await client.create_consumer(ConsumerSpec(c, s))
    await client.publish(s, "later.a", "delayed", delay=0.7)
    await client.publish(s, "later.a", "at", deliver_at=time.time() + 0.9)
    await client.publish(s, "later.a", "now")

    first = await client.pull(c, max_messages=10, expires=0.3)
    assert [x.text() for x in first] == ["now"]
    await client.ack(c, [x.offset for x in first])
    assert (await client.consumer_info(c)).num_delayed == 2

    due: list[str] = []

    async def all_due() -> bool:
        for msg in await client.pull(c, max_messages=10, expires=0.2):
            due.append(msg.text())
            msg.ack()
        return len(due) == 2

    await eventually(all_due)
    assert due == ["delayed", "at"]
    # A stateless read sees every record right away: delays apply to consumers.
    assert [r.text() for r in (await client.read(s)).records] == ["delayed", "at", "now"]


async def test_keeps_at_most_max_msgs_dropping_oldest_or_rejecting_new(client: ExspeedClient) -> None:
    old = uniq("max-old")
    await client.create_stream(StreamSpec(old, max_msgs=2))
    for v in ["1", "2", "3"]:
        await client.publish(old, "m.a", v)
    assert [r.text() for r in (await client.read(old)).records] == ["2", "3"]

    strict = uniq("max-new")
    await client.create_stream(StreamSpec(strict, max_msgs=2, discard="new"))
    await client.publish(strict, "m.a", "1")
    await client.publish(strict, "m.a", "2")
    with pytest.raises(ServerError) as e:
        await client.publish(strict, "m.a", "3")
    assert e.value.code == 429
    assert [r.text() for r in (await client.read(strict)).records] == ["1", "2"]
    cfg = (await client.stream_info(strict)).config
    assert (cfg.max_msgs, cfg.discard) == (2, "new")


async def test_keeps_max_msgs_per_subject(client: ExspeedClient) -> None:
    s = uniq("per-subject")
    await client.create_stream(StreamSpec(s, max_msgs_per_subject=1))
    for subject, v in [("a.x", "1"), ("a.y", "2"), ("a.x", "3")]:
        await client.publish(s, subject, v)
    assert [r.text() for r in (await client.read(s)).records] == ["2", "3"]


# ---------------------------------------------------------------------------
# Consumer routing
# ---------------------------------------------------------------------------


async def test_filters_by_headers_all_and_any(client: ExspeedClient) -> None:
    s = uniq("hdr")
    await client.create_stream(s)
    for region, tier, v in [("eu", "gold", "a"), ("us", "gold", "b"), ("eu", "free", "c"), ("asia", "free", "d")]:
        await client.publish(s, "e.x", v, headers={"region": region, "tier": tier})
    every = uniq("hdr-all")
    info = await client.create_consumer(ConsumerSpec(every, s, filter_headers={"region": "eu", "tier": "gold"}))
    assert info.spec.filter_headers == {"region": "eu", "tier": "gold"}
    assert [x.text() for x in await client.pull(every, expires=0.3)] == ["a"]

    some = uniq("hdr-any")
    await client.create_consumer(
        ConsumerSpec(some, s, filter_headers={"region": "eu", "tier": "gold"}, header_match="any")
    )
    assert [x.text() for x in await client.pull(some, expires=0.3)] == ["a", "b", "c"]


async def test_delivers_higher_priorities_first_within_the_window(client: ExspeedClient) -> None:
    s = uniq("prio")
    await client.create_stream(s)
    for v, priority in [("low1", 0), ("high1", 9), ("mid", 5), ("low2", 0), ("high2", 9)]:
        await client.publish(s, "t.x", v, priority=priority)
    c = uniq("prio-c")
    await client.create_consumer(ConsumerSpec(c, s, priority_window=100))
    order: list[str] = []
    while len(order) < 5:
        got = await client.pull(c, max_messages=10, expires=0.5)
        await client.ack(c, [x.offset for x in got])
        order.extend(x.text() for x in got)
    assert order == ["high1", "high2", "mid", "low1", "low2"]


async def test_single_active_consumer_refuses_pulls(client: ExspeedClient) -> None:
    s = uniq("single")
    await client.create_stream(s)
    c = uniq("single-c")
    info = await client.create_consumer(ConsumerSpec(c, s, single_active=True))
    assert info.spec.single_active is True
    with pytest.raises(ServerError):
        await client.pull(c, expires=0.2)


# ---------------------------------------------------------------------------
# Core pub/sub
# ---------------------------------------------------------------------------


async def test_fans_out_to_every_matching_subscription_and_stores_nothing(
    client: ExspeedClient, pair: tuple[ExspeedClient, ExspeedClient]
) -> None:
    a, b = pair
    every = await a.subscribe_core("orders.>")
    eu = await b.subscribe_core("orders.eu.*")
    await client.publish_core("orders.eu.created", {"id": 1}, headers={"trace-id": "t1"})
    await client.publish_core("orders.us.created", "2")
    await client.publish_core("billing.x", "ignored")

    m1 = await every.next(timeout=5)
    assert m1 is not None
    assert (m1.subject, m1.json(), m1.header("trace-id"), m1.reply_to) == ("orders.eu.created", {"id": 1}, "t1", None)
    m2 = await every.next(timeout=5)
    assert m2 is not None and m2.subject == "orders.us.created"
    m3 = await eu.next(timeout=5)
    assert m3 is not None and m3.subject == "orders.eu.created"
    assert await eu.next(timeout=0.2) is None
    assert await every.next(timeout=0.2) is None

    # A late subscriber sees only what comes next.
    late = await a.subscribe_core("orders.>")
    assert await late.next(timeout=0.2) is None
    await late.unsubscribe()
    await every.unsubscribe()
    await client.publish_core("orders.eu.created", "3")
    m4 = await eu.next(timeout=5)
    assert m4 is not None and m4.text() == "3"
    assert every.closed


async def test_splits_messages_across_a_queue_group(
    pair: tuple[ExspeedClient, ExspeedClient], client: ExspeedClient
) -> None:
    w1, w2 = pair
    subject = f"jobs.{uniq('q')}"
    s1 = await w1.subscribe_core(subject, queue="workers")
    s2 = await w2.subscribe_core(subject, queue="workers")
    for i in range(20):
        await client.publish_core(subject, str(i))

    async def count(s: CoreSubscription) -> int:
        n = 0
        while await s.next(timeout=0.3):
            n += 1
        return n

    n1, n2 = await count(s1), await count(s2)
    assert n1 + n2 == 20
    assert n1 > 0 and n2 > 0


# ---------------------------------------------------------------------------
# Request-reply
# ---------------------------------------------------------------------------


async def test_answers_requests_through_one_inbox_and_fails_fast_with_no_responders(
    client: ExspeedClient, exspeed_server: ExspeedServer
) -> None:
    async with exspeed_server.connect() as svc:  # type: ignore[attr-defined]
        reqs = await svc.subscribe_core("svc.upper", queue="svc")

        async def responder() -> None:
            async for msg in reqs:
                await msg.respond(msg.text().upper())

        task = asyncio.ensure_future(responder())
        r = await client.request("svc.upper", "hello", timeout=5)
        assert isinstance(r, CoreMessage)
        assert r.text() == "HELLO"
        many = await asyncio.gather(*(client.request("svc.upper", f"m{i}", timeout=5) for i in range(20)))
        assert [x.text() for x in many] == [f"M{i}" for i in range(20)]

        t = time.monotonic()
        with pytest.raises(ServerError) as e:
            await client.request("svc.nobody", "x", timeout=5)
        assert e.value.code == 404
        assert time.monotonic() - t < 2

        await reqs.unsubscribe()
        await task


async def test_request_times_out_when_a_responder_never_answers(
    client: ExspeedClient, exspeed_server: ExspeedServer
) -> None:
    async with exspeed_server.connect() as svc:  # type: ignore[attr-defined]
        subject = f"svc.{uniq('silent')}"
        silent = await svc.subscribe_core(subject)
        with pytest.raises(TimeoutError):
            await client.request(subject, "x", timeout=0.3)
        got = await silent.next(timeout=1)
        assert got is not None and got.reply_to is not None
        assert re.match(r"^_INBOX\.", got.reply_to)


# ---------------------------------------------------------------------------
# KV buckets
# ---------------------------------------------------------------------------


async def test_kv_puts_gets_compares_and_sets_deletes_and_lists_keys(client: ExspeedClient) -> None:
    kv = client.kv(uniq("cfg"))
    await kv.create(history=3)
    await kv.create(history=3)  # idempotent
    assert await kv.get("app.mode") is None

    r1 = await kv.put("app.mode", "dev")
    r2 = await kv.put("app.mode", {"mode": "prod"})
    assert (r1, r2) == (1, 2)
    e = await kv.get("app.mode")
    assert e is not None
    assert (e.key, e.revision, e.op, e.json()) == ("app.mode", r2, "put", {"mode": "prod"})
    old = await kv.get_revision("app.mode", r1)
    assert old is not None and old.text() == "dev"

    # Compare-and-set.
    created = await kv.create_key("app.port", "8080")
    with pytest.raises(ServerError) as dup:
        await kv.create_key("app.port", "9090")
    assert dup.value.code == 409
    updated = await kv.update("app.port", "9090", created)
    with pytest.raises(ServerError) as stale:
        await kv.update("app.port", "1", created)
    assert stale.value.code == 409
    assert stale.value.detail["current_revision"] == updated

    await kv.put("db.url", "postgres://")
    assert await kv.keys() == ["app.mode", "app.port", "db.url"]
    assert await kv.keys("app.*") == ["app.mode", "app.port"]

    deleted = await kv.delete("app.port")
    assert deleted > updated
    assert await kv.get("app.port") is None
    assert await kv.keys("app.*") == ["app.mode"]
    assert [(h.text(), h.op) for h in await kv.history("app.port")] == [
        ("8080", "put"),
        ("9090", "put"),
        ("", "delete"),
    ]
    # A deleted key can be created again.
    assert await kv.create_key("app.port", "7070") > deleted

    await kv.purge("app.mode")
    assert await kv.get("app.mode") is None

    with pytest.raises(ServerError) as missing:
        await client.kv(uniq("missing")).get("x")
    assert missing.value.code == 404

    await kv.destroy()
    with pytest.raises(ServerError) as gone:
        await client.stream_info(kv.stream)
    assert gone.value.code == 404


async def test_kv_expires_keys_after_their_ttl(client: ExspeedClient) -> None:
    kv = client.kv(uniq("ttl"))
    await kv.create()
    await kv.put("session.a", "x", ttl=0.2)
    await kv.put("session.b", "y")
    await asyncio.sleep(0.5)
    assert await kv.get("session.a") is None
    b = await kv.get("session.b")
    assert b is not None and b.text() == "y"


async def test_kv_bucket_ttl_expires_every_key(client: ExspeedClient) -> None:
    kv = client.kv(uniq("bttl"))
    await kv.create(ttl=0.2)
    await kv.put("k.a", "x")
    await asyncio.sleep(0.5)
    assert await kv.get("k.a") is None


async def test_kv_watches_current_values_first_then_every_change(client: ExspeedClient) -> None:
    kv = client.kv(uniq("watch"))
    await kv.create()
    await kv.put("user.1", "alice")
    await kv.put("user.2", "bob")
    await kv.put("user.1", "alice2")
    await kv.put("other.x", "filtered")
    await kv.put("user.3", "gone")
    await kv.delete("user.3")

    w: KvWatch = kv.watch("user.*")

    async def take(n: int) -> list[KvEntry]:
        out: list[KvEntry] = []
        while len(out) < n:
            e = await w.next(timeout=5)
            if e is None:
                raise AssertionError(f"watch stalled after {len(out)} entries")
            out.append(e)
        return out

    snapshot = await take(2)
    assert [(e.key, e.text(), e.revision) for e in snapshot] == [("user.2", "bob", 2), ("user.1", "alice2", 3)]

    await kv.put("user.4", "dave")
    await kv.put("other.y", "filtered")
    await kv.delete("user.2")
    changes = await take(2)
    assert [(e.key, e.op) for e in changes] == [("user.4", "put"), ("user.2", "delete")]
    assert await w.next(timeout=0.2) is None
    w.stop()
    assert await w.next() is None
    with contextlib.suppress(Exception):
        await w.aclose()
