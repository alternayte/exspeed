"""End-to-end tests against a real exspeed server: streams, publishing,
reads, consumers, queries, auth, reconnection and TLS."""

from __future__ import annotations

import asyncio
import tempfile
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from harness import ExspeedServer, ServerOptions, eventually, openssl, openssl_available, uniq

from exspeed import (
    ConnectionError,
    ConsumerSpec,
    ExspeedClient,
    Message,
    PublishRecord,
    PublishResult,
    ReconnectOptions,
    ServerError,
    StreamSpec,
    Subscription,
    TlsOptions,
    new_msg_id,
)


async def make_stream(client: ExspeedClient, prefix: str = "s") -> str:
    name = uniq(prefix)
    await client.create_stream(name)
    return name


async def publish_n(client: ExspeedClient, stream: str, n: int, subject: str = "work.item") -> None:
    await client.publish_batch(stream, [PublishRecord(subject, {"i": i}) for i in range(n)])


# ---------------------------------------------------------------------------
# Basics
# ---------------------------------------------------------------------------


async def test_pings_and_reports_metadata(client: ExspeedClient) -> None:
    assert await client.ping() >= 0
    md = await client.metadata()
    assert md.is_leader is True
    assert md.server_version == client.server_info.server_version
    assert md.node_id == client.server_info.node_id


async def test_manages_streams(client: ExspeedClient) -> None:
    name = uniq("admin")
    await client.create_stream(StreamSpec(name, max_age_secs=3600))
    await client.create_stream(StreamSpec(name, max_age_secs=3600))  # same settings: ok
    with pytest.raises(ServerError) as conflict:
        await client.create_stream(StreamSpec(name, max_age_secs=60))
    assert conflict.value.code == 409

    await client.publish(name, "admin.created", {"n": 1})
    info = await client.stream_info(name)
    assert (info.name, info.earliest_offset, info.next_offset, info.records, info.internal) == (name, 0, 1, 1, False)
    assert info.config.max_age_secs == 3600
    assert name in [s.name for s in await client.list_streams()]

    await client.update_stream(StreamSpec(name, max_age_secs=7200))
    assert (await client.stream_info(name)).config.max_age_secs == 7200

    c = uniq("admin-c")
    await client.create_consumer(ConsumerSpec(c, name))
    with pytest.raises(ServerError) as busy:
        await client.delete_stream(name)
    assert busy.value.code == 409
    assert busy.value.detail == {"consumers": [c]}
    await client.delete_consumer(c)
    await client.delete_stream(name)
    with pytest.raises(ServerError) as gone:
        await client.stream_info(name)
    assert gone.value.code == 404


async def test_rejects_internal_stream_names_and_unknown_streams(client: ExspeedClient) -> None:
    with pytest.raises(ServerError) as e:
        await client.create_stream("__nope")
    assert e.value.code == 403
    with pytest.raises(ServerError) as e:
        await client.publish(uniq("missing"), "x.y", "v")
    assert e.value.code == 404


# ---------------------------------------------------------------------------
# Publishing and reading
# ---------------------------------------------------------------------------


async def test_publishes_and_reads_back_with_subject_filters(client: ExspeedClient) -> None:
    s = await make_stream(client, "orders")
    subjects = ["orders.placed", "orders.shipped", "orders.eu.placed", "payments.done", "orders.placed"]
    for i, subject in enumerate(subjects):
        r = await client.publish(s, subject, {"i": i}, key=f"k{i}", headers={"x-index": str(i)})
        assert r == PublishResult(i, False)

    everything = await client.read(s)
    assert [r.subject for r in everything.records] == subjects
    assert (everything.next_offset, everything.high_watermark) == (5, 5)
    first = everything.records[0]
    assert first.json() == {"i": 0}
    assert first.key == b"k0"
    assert first.header("x-index") == "0"
    assert abs(first.timestamp_ms - time.time() * 1000) < 60_000
    assert first.timestamp.tzinfo is not None

    assert [r.offset for r in (await client.read(s, filter="orders.*")).records] == [0, 1, 4]
    assert [r.offset for r in (await client.read(s, filter="orders.>")).records] == [0, 1, 2, 4]
    page = await client.read(s, from_offset=1, max_records=2)
    assert [r.offset for r in page.records] == [1, 2]
    assert page.next_offset == 3

    with pytest.raises(ServerError) as bad:
        await client.read(s, filter="orders.>.x")
    assert bad.value.code == 400


async def test_verifies_record_crcs_when_asked(exspeed_server: ExspeedServer, client: ExspeedClient) -> None:
    s = await make_stream(client)
    await client.publish_batch(
        s, [PublishRecord("crc.check", {"i": i}, key="k", headers={"h": "v"}) for i in range(20)]
    )
    async with exspeed_server.connect(verify_crc=True) as checked:  # type: ignore[attr-defined]
        r = await checked.read(s)
        assert [x.json()["i"] for x in r.records] == list(range(20))
        c = uniq("crc-c")
        await checked.create_consumer(ConsumerSpec(c, s))
        msgs = await checked.pull(c, max_messages=20, expires=1)
        assert len(msgs) == 20
        assert all(x.delivery_count == 1 for x in msgs)


async def test_long_polls_a_read_until_new_data_arrives(client: ExspeedClient) -> None:
    s = await make_stream(client)
    start = time.monotonic()
    pending = asyncio.ensure_future(client.read(s, from_offset=0, wait=5))
    await asyncio.sleep(0.2)
    await client.publish(s, "late.arrival", "hello")
    r = await pending
    assert [x.text() for x in r.records] == ["hello"]
    assert time.monotonic() - start < 4


async def test_publishes_batches_and_deduplicates_by_msg_id(client: ExspeedClient) -> None:
    s = await make_stream(client)
    m1, m2, m3 = new_msg_id(), new_msg_id(), new_msg_id()
    first = await client.publish_batch(
        s, [PublishRecord("orders.placed", {"id": 1}, msg_id=m1), PublishRecord("orders.placed", {"id": 2}, msg_id=m2)]
    )
    assert first == [PublishResult(0, False), PublishResult(1, False)]
    retry = await client.publish_batch(
        s, [PublishRecord("orders.placed", {"id": 1}, msg_id=m1), PublishRecord("orders.placed", {"id": 3}, msg_id=m3)]
    )
    assert retry == [PublishResult(0, True), PublishResult(2, False)]
    assert await client.publish(s, "orders.placed", {"id": 2}, msg_id=m2) == PublishResult(1, True)

    with pytest.raises(ServerError) as reused:
        await client.publish(s, "orders.placed", {"id": 99}, msg_id=m1)
    assert reused.value.code == 409
    assert reused.value.detail == {"stored_offset": 0}
    assert (await client.stream_info(s)).next_offset == 3


async def test_keeps_the_coalescing_publishers_records_in_call_order(client: ExspeedClient) -> None:
    s = await make_stream(client)
    n = 1000
    async with client.publisher(max_batch_records=64) as p:
        results = await asyncio.gather(*(p.publish(s, "seq.value", {"i": i}) for i in range(n)))
    assert [r.offset for r in results] == list(range(n))

    seen: list[int] = []
    offset = 0
    while len(seen) < n:
        r = await client.read(s, from_offset=offset, max_records=500)
        seen.extend(rec.json()["i"] for rec in r.records)
        offset = r.next_offset
    assert seen == list(range(n))


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


async def test_creates_a_consumer_subscribes_receives_and_acks(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("billing")
    info = await client.create_consumer(ConsumerSpec(c, s, filter_subjects=["work.>"]))
    assert (info.spec.name, info.spec.stream, info.spec.filter_subjects) == (c, s, ["work.>"])
    assert (info.spec.deliver, info.spec.ack) == ("all", "explicit")
    # Idempotent for the same spec; 409 for a different one.
    await client.create_consumer(ConsumerSpec(c, s, filter_subjects=["work.>"]))
    with pytest.raises(ServerError) as e:
        await client.create_consumer(ConsumerSpec(c, s))
    assert e.value.code == 409
    assert [i.spec.name for i in await client.list_consumers(s)] == [c]

    await publish_n(client, s, 3)
    await client.publish(s, "other.thing", "filtered out")
    got: list[Message] = []
    async with client.subscribe(c, window=10) as sub:
        async for msg in sub:
            got.append(msg)
            msg.ack()
            if len(got) == 3:
                break
    assert [(x.offset, x.delivery_count, x.json()) for x in got] == [
        (0, 1, {"i": 0}),
        (1, 1, {"i": 1}),
        (2, 1, {"i": 2}),
    ]
    assert sub.end_reason is not None and sub.end_reason.code == 0

    async def acked() -> object:
        i = await client.consumer_info(c)
        return i if i.num_unacked == 0 and i.stats.acked == 3 else None

    after = await eventually(acked)
    assert after.ack_floor >= 3  # type: ignore[attr-defined]


async def test_never_pushes_more_than_the_credit_window(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("credit")
    await client.create_consumer(ConsumerSpec(c, s))
    await publish_n(client, s, 50)
    sub = await client.subscribe(c, window=4)

    async def full() -> bool:
        return sub.buffered == 4

    await eventually(full)
    await asyncio.sleep(0.3)
    assert sub.buffered == 4  # nothing beyond the window

    offsets: list[int] = []
    while len(offsets) < 50:
        msg = await sub.next(timeout=5)
        assert msg is not None
        assert sub.buffered <= 4
        offsets.append(msg.offset)
        msg.ack()
    assert offsets == list(range(50))
    await sub.unsubscribe()


async def test_redelivers_a_nacked_message_with_a_higher_delivery_count(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("nack")
    await client.create_consumer(ConsumerSpec(c, s))
    await client.publish(s, "work.item", "retry me")
    sub = await client.subscribe(c, window=10)
    first = await sub.next(timeout=5)
    assert first is not None and first.delivery_count == 1
    await first.nack()
    second = await sub.next(timeout=5)
    assert second is not None
    assert (second.offset, second.delivery_count) == (first.offset, 2)
    await second.nack(0.2)
    t = time.monotonic()
    third = await sub.next(timeout=5)
    assert third is not None and third.delivery_count == 3
    assert time.monotonic() - t >= 0.15
    await client.ack(c, [third.offset])  # confirmed ack
    assert (await client.consumer_info(c)).num_unacked == 0
    await sub.unsubscribe()


async def test_shares_work_between_two_clients_on_one_consumer(
    client: ExspeedClient, exspeed_server: ExspeedServer
) -> None:
    s = await make_stream(client)
    c = uniq("shared")
    await client.create_consumer(ConsumerSpec(c, s))
    other = await exspeed_server.connect(client_id="e2e-2")
    try:
        sub_a = await client.subscribe(c, window=8)
        sub_b = await other.subscribe(c, window=8)
        seen: dict[str, list[int]] = {"a": [], "b": []}
        redelivered: list[int] = []

        async def drain(name: str, sub: Subscription) -> None:
            async for msg in sub:
                if msg.delivery_count != 1:
                    redelivered.append(msg.offset)
                seen[name].append(msg.offset)
                msg.ack()
                await asyncio.sleep(0.001)  # let the other subscriber get a share

        done = asyncio.gather(drain("a", sub_a), drain("b", sub_b))
        total = 200
        await publish_n(client, s, total)

        async def all_seen() -> bool:
            return len(seen["a"]) + len(seen["b"]) >= total

        await eventually(all_seen, 15)
        await asyncio.sleep(0.2)  # anything extra would show up now
        await sub_a.unsubscribe()
        await sub_b.unsubscribe()
        await done
        assert seen["a"] and seen["b"]
        assert sorted(seen["a"] + seen["b"]) == list(range(total))
        assert redelivered == []
    finally:
        await other.close()


async def test_dead_letters_after_max_deliver_and_immediately_on_term(client: ExspeedClient) -> None:
    s = await make_stream(client)
    dlq = await make_stream(client, "dlq")
    c = uniq("dlq-c")
    await client.create_consumer(ConsumerSpec(c, s, max_deliver=2, dlq_stream=dlq))
    await client.publish(s, "work.poison", "bad")
    await client.publish(s, "work.terminal", "worse")

    [m1] = await client.pull(c, max_messages=1, expires=2)
    assert m1.delivery_count == 1
    await m1.nack()
    [m2] = await client.pull(c, max_messages=1, expires=2)
    assert (m2.offset, m2.delivery_count) == (0, 2)
    await m2.nack()

    [t] = await client.pull(c, max_messages=1, expires=2)
    assert t.offset == 1
    await t.term("cannot parse")

    async def dead_letters() -> object:
        r = await client.read(dlq)
        return r.records if len(r.records) == 2 else None

    dead = await eventually(dead_letters)
    assert [d.text() for d in dead] == ["bad", "worse"]  # type: ignore[attr-defined]
    d0, d1 = dead  # type: ignore[misc]
    assert d0.header("exspeed-dlq-origin") == c
    assert d0.header("exspeed-dlq-stream") == s
    assert d0.header("exspeed-dlq-original-offset") == "0"
    assert d0.header("exspeed-dlq-deliveries") == "2"
    assert d0.header("exspeed-dlq-cause") == "max_deliver"
    assert d1.header("exspeed-dlq-original-offset") == "1"
    assert d1.header("exspeed-dlq-cause") == "rejected"
    assert "cannot parse" in (d1.header("exspeed-dlq-reason") or "")
    info = await client.consumer_info(c)
    assert info.stats.dead_lettered == 2
    assert await client.pull(c, expires=0.3) == []


async def test_long_polls_a_pull_and_times_out_empty(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("pull")
    await client.create_consumer(ConsumerSpec(c, s))

    t0 = time.monotonic()
    assert await client.pull(c, expires=0.3) == []
    assert time.monotonic() - t0 >= 0.25

    t1 = time.monotonic()
    pending = asyncio.ensure_future(client.pull(c, max_messages=10, expires=10))
    await asyncio.sleep(0.2)
    await client.publish(s, "work.item", "now")
    msgs = await pending
    assert [x.text() for x in msgs] == ["now"]
    assert time.monotonic() - t1 < 5
    # A long pull doesn't block other requests on the same connection.
    slow = asyncio.ensure_future(client.pull(c, expires=1))
    assert await client.ping() < 0.5
    await slow
    msgs[0].ack()


async def test_seeks_a_consumer(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("seek")
    await client.create_consumer(ConsumerSpec(c, s, ack="none"))
    await publish_n(client, s, 5)

    async def offsets() -> list[int]:
        return [x.offset for x in await client.pull(c, max_messages=100, expires=0.3)]

    assert await offsets() == [0, 1, 2, 3, 4]
    await client.seek(c, 2)
    assert await offsets() == [2, 3, 4]
    await client.seek(c, "earliest")
    assert await offsets() == [0, 1, 2, 3, 4]
    await client.seek(c, "latest")
    assert await offsets() == []
    await client.seek(c, datetime.fromtimestamp(0, tz=timezone.utc))
    assert await offsets() == [0, 1, 2, 3, 4]
    await client.seek(c, datetime.now(timezone.utc) + timedelta(minutes=1))
    assert await offsets() == []
    with pytest.raises(ServerError) as e:
        await client.seek(uniq("nobody"), "earliest")
    assert e.value.code == 404


async def test_removes_an_ephemeral_consumer_when_its_connection_closes(
    client: ExspeedClient, exspeed_server: ExspeedServer
) -> None:
    s = await make_stream(client)
    c = uniq("eph")
    owner = await exspeed_server.connect()
    await owner.create_consumer(ConsumerSpec(c, s, ephemeral=True, deliver="new"))
    assert (await client.consumer_info(c)).spec.ephemeral is True
    await owner.close()

    async def gone() -> object:
        try:
            await client.consumer_info(c)
        except ServerError as e:
            return e
        return None

    err = await eventually(gone)
    assert err.code == 404  # type: ignore[attr-defined]


async def test_ends_subscriptions_with_404_when_the_consumer_is_deleted(client: ExspeedClient) -> None:
    s = await make_stream(client)
    c = uniq("gone")
    await client.create_consumer(ConsumerSpec(c, s))
    sub = await client.subscribe(c)
    nxt = asyncio.ensure_future(sub.next())
    await client.delete_consumer(c)
    assert await nxt is None
    assert sub.end_reason is not None and sub.end_reason.code == 404


async def test_runs_sql_queries(client: ExspeedClient) -> None:
    s = uniq("q").replace("-", "_")
    await client.create_stream(s)
    await client.publish_batch(
        s, [PublishRecord("metrics.cpu", {"region": "us" if i == 2 else "eu", "i": i}) for i in (1, 2, 3)]
    )
    r = await client.query(f'SELECT COUNT(*) AS cnt FROM "{s}"')
    assert r.columns == ["cnt"]
    assert r.rows == [[3]]
    assert r.row_count == 1
    assert isinstance(r.execution_time_ms, int)
    with pytest.raises(ServerError) as bad:
        await client.query("SELEKT nonsense")
    assert bad.value.code == 400


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def auth_server() -> Iterator[ExspeedServer]:
    s = ExspeedServer.start(ServerOptions(auth_token="s3cret-token"))
    try:
        yield s
    finally:
        s.stop()


async def test_auth_rejects_a_wrong_or_missing_token_with_401(auth_server: ExspeedServer) -> None:
    for token in ["wrong", None]:
        with pytest.raises(ServerError) as e:
            await auth_server.connect(token=token)
        assert e.value.code == 401


async def test_auth_accepts_the_right_token(auth_server: ExspeedServer) -> None:
    async with auth_server.connect(token="s3cret-token") as c:  # type: ignore[attr-defined]
        s = uniq("authed")
        await c.create_stream(s)
        assert (await c.publish(s, "auth.ok", "yes")).offset == 0
        assert (await c.query(f'SELECT COUNT(*) AS n FROM "{s}"')).rows == [[1]]


# ---------------------------------------------------------------------------
# Reconnection
# ---------------------------------------------------------------------------


@pytest.fixture
def restartable_server() -> Iterator[ExspeedServer]:
    s = ExspeedServer.start()
    try:
        yield s
    finally:
        s.stop()


async def test_resubscribes_after_the_server_restarts(restartable_server: ExspeedServer) -> None:
    server = restartable_server
    client = await server.connect(reconnect=ReconnectOptions(initial_delay=0.05, max_delay=0.2))
    try:
        s = uniq("durable")
        c = uniq("durable-c")
        await client.create_stream(s)
        await client.create_consumer(ConsumerSpec(c, s))
        await client.publish(s, "work.item", "before")
        sub = await client.subscribe(c, window=10)
        m1 = await sub.next(timeout=5)
        assert m1 is not None and m1.text() == "before"  # not acked

        events: list[str] = []
        reconnected = asyncio.get_running_loop().create_future()
        client.on("disconnect", lambda e: events.append("disconnect"))
        client.on("reconnect", lambda info: reconnected.done() or reconnected.set_result(info))
        await asyncio.to_thread(server.restart)
        await asyncio.wait_for(reconnected, 30)
        assert events == ["disconnect"]
        assert client.connected

        again = await sub.next(timeout=10)
        assert again is not None and again.text() == "before"
        # (The delivery count may restart at 1: the server persists consumer
        # state in periodic snapshots.)
        again.ack()
        await client.publish(s, "work.item", "after")
        m2 = await sub.next(timeout=5)
        assert m2 is not None and m2.text() == "after"
        m2.ack()
        assert not sub.closed
    finally:
        await client.close()


async def test_fails_requests_with_connection_error_while_disconnected(restartable_server: ExspeedServer) -> None:
    client = await restartable_server.connect(reconnect=False)
    closed = asyncio.get_running_loop().create_future()
    client.on("close", lambda e: closed.done() or closed.set_result(e))
    await asyncio.to_thread(restartable_server.restart)
    await asyncio.wait_for(closed, 10)
    with pytest.raises(ConnectionError):
        await client.ping()


# ---------------------------------------------------------------------------
# TLS
# ---------------------------------------------------------------------------

needs_openssl = pytest.mark.skipif(not openssl_available(), reason="openssl CLI not available")


@pytest.fixture(scope="module")
def tls_server() -> Iterator[tuple[ExspeedServer, Path]]:
    with tempfile.TemporaryDirectory(prefix="exspeed-py-tls-") as tmp:
        yield from _tls_server(Path(tmp))


def _tls_server(d: Path) -> Iterator[tuple[ExspeedServer, Path]]:
    openssl(
        d, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", "key.pem", "-out", "cert.pem",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    )  # fmt: skip
    s = ExspeedServer.start(ServerOptions(tls_cert=str(d / "cert.pem"), tls_key=str(d / "key.pem")))
    try:
        yield s, d
    finally:
        s.stop()


@needs_openssl
async def test_connects_over_tls_with_a_custom_ca(tls_server: tuple[ExspeedServer, Path]) -> None:
    server, d = tls_server
    c = await server.connect(tls=TlsOptions(ca_file=str(d / "cert.pem")))
    try:
        s = uniq("tls")
        await c.create_stream(s)
        assert (await c.publish(s, "tls.ok", "secure")).offset == 0
    finally:
        await c.close()
    # CA given as PEM text, verified against the "localhost" name.
    pem = (d / "cert.pem").read_text()
    c = await server.connect(host="127.0.0.1", tls=TlsOptions(ca_data=pem, server_hostname="localhost"))
    await c.close()


@needs_openssl
async def test_refuses_an_untrusted_certificate_and_plain_tcp(tls_server: tuple[ExspeedServer, Path]) -> None:
    server, _ = tls_server
    with pytest.raises(ConnectionError):
        await server.connect(tls=True, request_timeout=3)
    with pytest.raises(ConnectionError):
        await server.connect(request_timeout=3)


@pytest.fixture(scope="module")
def mtls_server() -> Iterator[tuple[ExspeedServer, Path]]:
    with tempfile.TemporaryDirectory(prefix="exspeed-py-mtls-") as tmp:
        yield from _mtls_server(Path(tmp))


def _mtls_server(d: Path) -> Iterator[tuple[ExspeedServer, Path]]:
    # One CA signs both the server's and the client's certificate.
    openssl(
        d, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", "ca.key", "-out", "ca.pem",
        "-subj", "/CN=exspeed-test-ca",
        # Python 3.13+ verifies with VERIFY_X509_STRICT, which wants these on a CA.
        "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    )  # fmt: skip
    for name, cn, san in [
        ("server", "localhost", "subjectAltName=DNS:localhost,IP:127.0.0.1"),
        ("client", "orders.internal", "subjectAltName=DNS:orders.internal"),
    ]:
        openssl(d, "req", "-newkey", "rsa:2048", "-nodes", "-keyout", f"{name}.key", "-out", f"{name}.csr",
                "-subj", f"/CN={cn}")  # fmt: skip
        (d / f"{name}.ext").write_text(f"{san}\nauthorityKeyIdentifier=keyid,issuer\n")
        openssl(
            d, "x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
            "-days", "1", "-out", f"{name}.pem", "-extfile", f"{name}.ext",
        )  # fmt: skip
    s = ExspeedServer.start(
        ServerOptions(tls_cert=str(d / "server.pem"), tls_key=str(d / "server.key"), tls_client_ca=str(d / "ca.pem"))
    )
    try:
        yield s, d
    finally:
        s.stop()


@needs_openssl
async def test_connects_with_a_client_certificate(mtls_server: tuple[ExspeedServer, Path]) -> None:
    server, d = mtls_server
    tls = TlsOptions(ca_file=str(d / "ca.pem"), cert_file=str(d / "client.pem"), key_file=str(d / "client.key"))
    async with server.connect(tls=tls) as c:  # type: ignore[attr-defined]
        s = uniq("mtls")
        await c.create_stream(s)
        assert (await c.publish(s, "mtls.ok", "mutual")).offset == 0


@needs_openssl
async def test_mtls_is_refused_without_a_client_certificate(mtls_server: tuple[ExspeedServer, Path]) -> None:
    server, d = mtls_server
    with pytest.raises(ConnectionError):
        await server.connect(tls=TlsOptions(ca_file=str(d / "ca.pem")), request_timeout=3)
