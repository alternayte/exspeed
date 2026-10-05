"""Codec tests. The byte fixtures come from the Rust encoder
(crates/exspeed-protocol/src/client.rs) and match the TypeScript SDK's."""

from __future__ import annotations

import json
import re
import struct
from datetime import datetime, timezone

import pytest

from exspeed import (
    ConsumerSpec,
    DeliverFromOffset,
    DeliverFromTime,
    ExspeedError,
    ProtocolError,
    PublishRecord,
    StreamSpec,
)
from exspeed.protocol import (
    MAX_PAYLOAD_SIZE,
    FrameParser,
    OpCode,
    Reader,
    SeekKind,
    WirePublishRecord,
    WireRecord,
    WireStreamSpec,
    Writer,
    crc32c,
    decode_request,
    decode_response,
    encode_frame,
    encode_record,
    encode_request,
    encode_response,
    request_frame,
    response_frame,
    verify_record_crc,
)
from exspeed.protocol import messages as m


def hx(s: str) -> bytes:
    """Hex string (spaces and ``|`` ignored) to bytes."""
    return bytes.fromhex(re.sub(r"[\s|]", "", s))


def rec(i: int) -> WireRecord:
    """Same as ``rec(i)`` in the Rust protocol tests."""
    return WireRecord(
        offset=i,
        timestamp_ns=1_700_000_000_000_000_000 + i,
        delivery_count=i % 3,
        subject=f"orders.{i}",
        key=f"k{i}".encode() if i % 2 == 0 else None,
        value=bytes([i]) * (i % 7),
        headers=[("h", f"v{i}")],
    )


pr = WirePublishRecord(subject="a.b", key=b"k", value=b'{"x":1}', headers=[("h1", "v1")], msg_id="m-1")
empty_pr = WirePublishRecord(subject="", key=None, value=b"", headers=[], msg_id=None)
spec = WireStreamSpec("s", max_age_secs=1, max_bytes=2, dedup_window_secs=3, dedup_max_entries=4, compaction=True)
#: Same as `limited` in the Rust round-trip test.
limited_spec = StreamSpec(
    name="q",
    max_msgs=10,
    discard="new",
    max_msgs_per_subject=1,
    allow_msg_ttl=True,
    msg_ttl_ms=5000,
    allow_delayed=True,
    retention="work_queue",
).to_wire()

consumer_spec = ConsumerSpec(
    name="c",
    stream="s",
    filter_subjects=["orders.>"],
    deliver=DeliverFromTime(123),
    ack="explicit",
    ack_wait_ms=30000,
    max_deliver=5,
    backoff_ms=[100, 1000],
    max_ack_pending=1000,
    dlq_stream="s-dlq",
    ephemeral=False,
    dead_letter_expired=False,
    header_match="all",
    single_active=False,
    priority_window=0,
).to_wire()

ALL_REQUESTS: list[m.Request] = [
    m.Connect("c", "t"),
    m.Connect("c", None),
    m.Ping(),
    m.Metadata(),
    m.Publish("s", pr),
    m.PublishBatch("s", [pr, empty_pr]),
    m.CreateStream(spec),
    m.UpdateStream(spec),
    m.DeleteStream("s"),
    m.StreamInfo("s"),
    m.ListStreams(),
    m.Query("SELECT 1"),
    m.CreateConsumer(consumer_spec),
    m.DeleteConsumer("c"),
    m.ConsumerInfo("c"),
    m.ListConsumers(None),
    m.ListConsumers("s"),
    m.SeekConsumer("c", SeekKind.TIME, 9),
    m.SeekConsumer("c", SeekKind.LATEST, 0),
    m.Subscribe("c", 100),
    m.Credit(3, 10),
    m.Unsubscribe(3),
    m.Pull("c", 10, 1024, 500),
    m.Ack("c", [1, 2, 3]),
    m.Nack("c", 4, 100),
    m.Term("c", 5, "bad"),
    m.InProgress("c", [6]),
    m.Read("s", 7, 100, 1 << 20, 1000, "a.*"),
    m.CreateStream(limited_spec),
    m.CorePublish("a.b", "r", [("h", "v")], b"x"),
    m.CorePublish("a", None, [], b""),
    m.CoreSubscribe("a.*", "q"),
    m.CoreSubscribe("a.*", None),
    m.KvCreateBucket("b", 5, 1000, 0),
    m.KvPut("b", "k", b"v", 0, None),
    m.KvPut("b", "k", b"v", None, 500),
    m.KvGet("b", "k", 3),
    m.KvGet("b", "k", None),
    m.KvDelete("b", "k", True, 7),
    m.KvDelete("b", "k", False, None),
    m.KvKeys("b", "a.*"),
    m.KvHistory("b", "k"),
]

ALL_RESPONSES: list[m.Response] = [
    m.Ok(),
    m.Pong(),
    m.Error(404, "nope", None),
    m.Error(503, "not leader", b'{"leader":"h:1"}'),
    m.ConnectOk("0.6.0", "n1", "h:5933"),
    m.PublishOk(9, True),
    m.PublishBatchOk([(1, False), (1, True)]),
    m.SubscribeOk(2),
    m.Deliver(2, [rec(i) for i in range(5)]),
    m.SubscriptionEnded(2, 404, "consumer deleted"),
    m.Messages([rec(1)]),
    m.ReadResult(10, 12, [rec(i) for i in range(3)]),
    m.Json(b'{"a":1}'),
    m.CoreMsg(0x80000001, "a", "r", [("h", "v")], b"x"),
    m.CoreMsg(0x80000002, "a.b", None, [], b""),
]


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def test_round_trips_every_primitive() -> None:
    w = Writer()
    w.u8(255).u16(65535).u32(0xFFFFFFFF).u64(0xFFFFFFFFFFFFFFFF).u64(0)
    w.string("héllo").long_string("SELECT 'ü'").byte_string(b"raw")
    w.optional(None, Writer.string).optional("x", Writer.string)
    w.headers([("k", "v"), ("k", "v2")])
    r = Reader(w.finish())
    assert r.u8() == 255
    assert r.u16() == 65535
    assert r.u32() == 0xFFFFFFFF
    assert r.u64() == 0xFFFFFFFFFFFFFFFF
    assert r.u64() == 0
    assert r.string() == "héllo"
    assert r.long_string() == "SELECT 'ü'"
    assert r.byte_string() == b"raw"
    assert r.optional(Reader.string) is None
    assert r.optional(Reader.string) == "x"
    assert r.headers() == [("k", "v"), ("k", "v2")]
    r.finish()


def test_encodes_str_as_u16_length_plus_utf8_little_endian() -> None:
    assert Writer().string("é").finish() == hx("0200 c3a9")
    assert Writer().u64(0x0102030405060708).finish() == hx("0807060504030201")


@pytest.mark.parametrize(
    ("data", "read", "match"),
    [
        ("01", lambda r: r.u16(), "truncated"),
        ("0500 6162", lambda r: r.string(), "truncated"),
        ("00", lambda r: r.finish(), "trailing"),
        ("02", lambda r: r.optional(Reader.u8), "option flag"),
        ("0200 c328", lambda r: r.string(), "UTF-8"),
        ("ffff", lambda r: r.headers(), "header count"),
    ],
)
def test_rejects_truncated_trailing_and_malformed_input(data: str, read: object, match: str) -> None:
    with pytest.raises(ProtocolError, match=match):
        read(Reader(hx(data)))  # type: ignore[operator]


def test_rejects_values_that_dont_fit_the_wire_types() -> None:
    with pytest.raises(ExspeedError):
        Writer().string("x" * 70_000)
    with pytest.raises(ExspeedError):
        Writer().u64(-1)
    with pytest.raises(ExspeedError):
        Writer().u64(1 << 64)
    with pytest.raises(ExspeedError):
        Writer().u32(1.5)  # type: ignore[arg-type]
    with pytest.raises(ExspeedError):
        Writer().u8(True)


# ---------------------------------------------------------------------------
# Frames
# ---------------------------------------------------------------------------


def test_encodes_the_10_byte_header() -> None:
    assert encode_frame(0x01, 7, hx("aabb")) == hx("02 01 07000000 02000000 aabb")


def test_parses_frames_split_at_every_byte_and_several_per_chunk() -> None:
    stream = (
        response_frame(m.Pong(), 1)
        + response_frame(m.PublishOk(3, False), 2)
        + response_frame(m.Deliver(9, [rec(2)]), 0)
    )
    p = FrameParser()
    frames = []
    for i in range(len(stream)):
        frames.extend(p.push(stream[i : i + 1]))
    assert [(f.opcode, f.correlation_id) for f in frames] == [
        (OpCode.PONG, 1),
        (OpCode.PUBLISH_OK, 2),
        (OpCode.DELIVER, 0),
    ]
    assert p.pending == 0
    assert len(FrameParser().push(stream)) == 3


def test_rejects_a_bad_version_or_an_oversize_length() -> None:
    with pytest.raises(ProtocolError, match="version"):
        FrameParser().push(hx("01 80 00000000 00000000"))
    big = struct.pack("<BBII", 2, 0x80, 0, MAX_PAYLOAD_SIZE + 1)
    with pytest.raises(ProtocolError, match="too large"):
        FrameParser().push(big)
    with pytest.raises(ExspeedError, match="too large"):
        encode_frame(0x10, 1, bytes(MAX_PAYLOAD_SIZE + 1))


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("req", ALL_REQUESTS, ids=lambda r: type(r).__name__)
def test_request_round_trips(req: m.Request) -> None:
    frame = request_frame(req, 42)
    [f] = FrameParser().push(frame)
    assert f.opcode == req.OPCODE
    assert f.correlation_id == 42
    assert decode_request(f.opcode, f.payload) == req


def test_rejects_truncated_and_trailing_request_payloads() -> None:
    with pytest.raises(ProtocolError, match="trailing"):
        decode_request(OpCode.PING, hx("09"))
    sub = encode_request(m.Subscribe("c", 1))
    with pytest.raises(ProtocolError, match="truncated"):
        decode_request(OpCode.SUBSCRIBE, sub[:-1])


def test_rejects_hostile_counts_without_allocating() -> None:
    payload = Writer().string("s").u32(0xFFFFFFFF).finish()
    with pytest.raises(ProtocolError, match="exceeds payload"):
        decode_request(OpCode.PUBLISH_BATCH, payload)


LIMITS_JSON = (
    b'{"max_msgs":10,"discard":"new","max_msgs_per_subject":1,"allow_msg_ttl":true,'
    b'"msg_ttl_ms":5000,"allow_delayed":true,"retention":"work_queue"}'
)

REQUEST_FIXTURES: list[tuple[str, m.Request, str]] = [
    ("Connect", m.Connect("c", "t"), "0100 63 | 01 0100 74"),
    ("Connect without token", m.Connect("c", None), "0100 63 | 00"),
    ("Ping", m.Ping(), ""),
    (
        "Publish",
        m.Publish("s", pr),
        """0100 73
           0300 612e62
           01 01000000 6b
           07000000 7b2278223a317d
           0100 0200 6831 0200 7631
           01 0300 6d2d31""",
    ),
    (
        "PublishBatch",
        m.PublishBatch("s", [pr, empty_pr]),
        """0100 73 | 02000000
           0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31
           0000 00 00000000 0000 00""",
    ),
    (
        "CreateStream",
        m.CreateStream(spec),
        "0100 73 0100000000000000 0200000000000000 0300000000000000 0400000000000000 01",
    ),
    ("Query", m.Query("SELECT 1"), "08000000 53454c4543542031"),
    ("ListConsumers all", m.ListConsumers(None), "00"),
    ("ListConsumers stream", m.ListConsumers("s"), "01 0100 73"),
    ("Seek time", m.SeekConsumer("c", SeekKind.TIME, 9), "0100 63 03 0900000000000000"),
    ("Seek latest", m.SeekConsumer("c", SeekKind.LATEST, 0), "0100 63 01 0000000000000000"),
    ("Subscribe", m.Subscribe("c", 100), "0100 63 64000000"),
    ("Credit", m.Credit(3, 10), "03000000 0a000000"),
    ("Unsubscribe", m.Unsubscribe(3), "03000000"),
    ("Pull", m.Pull("c", 10, 1024, 500), "0100 63 0a000000 00040000 f4010000"),
    (
        "Ack",
        m.Ack("c", [1, 2, 3]),
        "0100 63 03000000 0100000000000000 0200000000000000 0300000000000000",
    ),
    ("Nack", m.Nack("c", 4, 100), "0100 63 0400000000000000 64000000"),
    ("Term", m.Term("c", 5, "bad"), "0100 63 0500000000000000 0300 626164"),
    (
        "Read",
        m.Read("s", 7, 100, 1 << 20, 1000, "a.*"),
        "0100 73 0700000000000000 64000000 00001000 e8030000 0300 612e2a",
    ),
    # Generated with the Rust encoder (Request::into_frame).
    (
        "CreateStream with limits",
        m.CreateStream(limited_spec),
        "0100 71 0000000000000000 0000000000000000 0000000000000000 0000000000000000 00 8d000000 " + LIMITS_JSON.hex(),
    ),
    (
        "CorePublish",
        m.CorePublish("a.b", "r", [("h", "v")], b"x"),
        "0300 612e62 | 01 0100 72 | 0100 0100 68 0100 76 | 01000000 78",
    ),
    ("CorePublish bare", m.CorePublish("a", None, [], b""), "0100 61 | 00 | 0000 | 00000000"),
    ("CoreSubscribe", m.CoreSubscribe("a.*", "q"), "0300 612e2a 01 0100 71"),
    ("CoreSubscribe bare", m.CoreSubscribe("a.*", None), "0300 612e2a 00"),
    (
        "KvCreateBucket",
        m.KvCreateBucket("b", 5, 1000, 0),
        "0100 62 0500000000000000 e803000000000000 0000000000000000",
    ),
    (
        "KvPut expecting revision 0",
        m.KvPut("b", "k", b"v", 0, None),
        "0100 62 0100 6b 01000000 76 01 0000000000000000 00",
    ),
    ("KvPut with TTL", m.KvPut("b", "k", b"v", None, 500), "0100 62 0100 6b 01000000 76 00 01 f401000000000000"),
    ("KvGet at revision", m.KvGet("b", "k", 3), "0100 62 0100 6b 01 0300000000000000"),
    ("KvGet", m.KvGet("b", "k", None), "0100 62 0100 6b 00"),
    ("KvDelete purge", m.KvDelete("b", "k", True, 7), "0100 62 0100 6b 01 01 0700000000000000"),
    ("KvDelete", m.KvDelete("b", "k", False, None), "0100 62 0100 6b 00 00"),
    ("KvKeys", m.KvKeys("b", "a.*"), "0100 62 0300 612e2a"),
    ("KvHistory", m.KvHistory("b", "k"), "0100 62 0100 6b"),
]


@pytest.mark.parametrize(("name", "req", "expected"), REQUEST_FIXTURES, ids=[f[0] for f in REQUEST_FIXTURES])
def test_request_matches_the_rust_encoding_byte_for_byte(name: str, req: m.Request, expected: str) -> None:
    assert encode_request(req).hex() == hx(expected).hex()


def test_connect_frame_matches_byte_for_byte_header_included() -> None:
    frame = request_frame(m.Connect("c", "t"), 1)
    assert frame == hx("02 01 01000000 07000000 | 0100 63 01 0100 74")


def test_consumer_spec_json_matches_serde_json_output() -> None:
    # serde_json::to_vec(&spec) in the Rust round-trip test.
    rust = (
        '{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},'
        '"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],'
        '"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,'
        '"dead_letter_expired":false,"header_match":"all","single_active":false,"priority_window":0}'
    )
    payload = encode_request(m.CreateConsumer(consumer_spec))
    assert struct.unpack_from("<I", payload)[0] == len(rust)
    assert payload[4:].decode() == rust


def test_maps_header_filters_single_active_and_priority_like_serde_json() -> None:
    full = ConsumerSpec(
        name="c",
        stream="s",
        filter_subjects=["orders.>"],
        deliver=DeliverFromTime(123),
        ack="explicit",
        ack_wait_ms=30000,
        max_deliver=5,
        backoff_ms=[100, 1000],
        max_ack_pending=1000,
        dlq_stream="s-dlq",
        ephemeral=False,
        dead_letter_expired=True,
        filter_headers={"tenant": "acme"},
        header_match="any",
        single_active=True,
        priority_window=50,
    ).to_wire()
    assert json.dumps(full, separators=(",", ":")) == (
        '{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},'
        '"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],'
        '"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,"dead_letter_expired":true,'
        '"filter_headers":{"tenant":"acme"},"header_match":"any","single_active":true,"priority_window":50}'
    )
    # An empty header filter is left out, as serde does.
    assert ConsumerSpec(name="c", stream="s", filter_headers={}).to_wire() == {"name": "c", "stream": "s"}


def test_omits_unset_consumer_spec_fields() -> None:
    assert ConsumerSpec("c", "s").to_wire() == {"name": "c", "stream": "s"}
    assert ConsumerSpec("c", "s", deliver=DeliverFromOffset(5), ack="none").to_wire() == {
        "name": "c",
        "stream": "s",
        "deliver": {"from_offset": 5},
        "ack": "none",
    }
    t = datetime.fromtimestamp(0.042, tz=timezone.utc)
    assert ConsumerSpec("c", "s", deliver=DeliverFromTime(t)).to_wire()["deliver"] == {"from_time": 42}
    assert ConsumerSpec("c", "s", deliver="new").to_wire()["deliver"] == "new"
    with pytest.raises(ExspeedError):
        ConsumerSpec("", "s").to_wire()
    with pytest.raises(ExspeedError):
        ConsumerSpec("c", "").to_wire()


# ---------------------------------------------------------------------------
# Stream limits
# ---------------------------------------------------------------------------


def test_sends_no_limits_trailer_when_every_limit_is_default() -> None:
    plain = StreamSpec("s", max_age_secs=1, discard="old", retention="limits", allow_msg_ttl=False).to_wire()
    assert plain.limits is None
    payload = encode_request(m.CreateStream(plain))
    assert payload == hx("0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00")
    # And an old-style spec without the trailer decodes without limits.
    assert decode_request(OpCode.CREATE_STREAM, payload) == m.CreateStream(WireStreamSpec("s", max_age_secs=1))


def test_sends_every_limit_serde_style_once_any_one_is_set() -> None:
    one = StreamSpec("s", allow_delayed=True).to_wire()
    assert json.dumps(one.limits, separators=(",", ":")) == (
        '{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,'
        '"msg_ttl_ms":0,"allow_delayed":true,"retention":"limits"}'
    )
    for kw in [
        {"max_msgs": 1},
        {"discard": "new"},
        {"max_msgs_per_subject": 2},
        {"allow_msg_ttl": True},
        {"msg_ttl_ms": 3},
        {"retention": "interest"},
    ]:
        assert StreamSpec("s", **kw).to_wire().limits is not None  # type: ignore[arg-type]
    with pytest.raises(ExspeedError):
        StreamSpec("").to_wire()


# ---------------------------------------------------------------------------
# Publish options
# ---------------------------------------------------------------------------


def test_publish_options_become_the_same_headers_as_the_rust_builders() -> None:
    r = PublishRecord(
        "a",
        "v",
        headers={"trace-id": "t"},
        ttl=0.5,
        delay="2000ms",
        deliver_at=1_700_000_000.0,
        priority=7,
    ).to_wire()
    # PublishRecord::new("a", "v").ttl(500ms).delay(2s).deliver_at(1700000000000).priority(7)
    assert r.headers == [
        ("trace-id", "t"),
        ("exspeed-ttl", "500ms"),
        ("exspeed-delay", "2000ms"),
        ("exspeed-deliver-at", "1700000000000"),
        ("exspeed-priority", "7"),
    ]


def test_publish_options_accept_strings_timedeltas_datetimes_and_reject_bad_values() -> None:
    from datetime import timedelta

    r = PublishRecord("a", "v", ttl="30s", delay=0, deliver_at=datetime.fromtimestamp(0.042, tz=timezone.utc)).to_wire()
    assert r.headers == [("exspeed-ttl", "30s"), ("exspeed-delay", "0ms"), ("exspeed-deliver-at", "42")]
    assert PublishRecord("a", "v", ttl=0.0002).to_wire().headers == [("exspeed-ttl", "1ms")]
    assert PublishRecord("a", "v", delay=timedelta(seconds=0.2)).to_wire().headers == [("exspeed-delay", "200ms")]
    assert PublishRecord("a", "v", ttl=timedelta(minutes=5)).to_wire().headers == [("exspeed-ttl", "300000ms")]
    for bad in [
        {"ttl": "soon"},
        {"delay": -1},
        {"priority": 10},
        {"priority": 1.5},
        {"priority": True},
        {"deliver_at": -5},
        {"ttl": float("nan")},
    ]:
        with pytest.raises(ExspeedError):
            PublishRecord("a", "v", **bad).to_wire()  # type: ignore[arg-type]


def test_values_bytes_as_is_strings_utf8_objects_json() -> None:
    assert PublishRecord("a", b"\x01\x02").to_wire().value == b"\x01\x02"
    assert PublishRecord("a", bytearray(b"\x03")).to_wire().value == b"\x03"
    assert PublishRecord("a", "hé").to_wire().value == "hé".encode()
    assert PublishRecord("a", {"id": 1, "x": [1, None]}).to_wire().value == b'{"id":1,"x":[1,null]}'
    assert PublishRecord("a", None).to_wire().value == b"null"
    assert PublishRecord("a", 1, key="k").to_wire().key == b"k"
    with pytest.raises(ExspeedError):
        PublishRecord("a", object()).to_wire()


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("resp", ALL_RESPONSES, ids=lambda r: type(r).__name__)
def test_response_round_trips(resp: m.Response) -> None:
    [f] = FrameParser().push(response_frame(resp, 7))
    assert f.opcode == resp.OPCODE
    assert decode_response(f.opcode, f.payload, verify_crc=True) == resp


def test_rejects_hostile_record_counts_and_unknown_opcodes() -> None:
    payload = Writer().u64(0).u64(0).u32(0xFFFFFFFF).finish()
    with pytest.raises(ProtocolError, match="exceeds payload"):
        decode_response(OpCode.READ_RESULT, payload)
    with pytest.raises(ProtocolError, match="not a server response"):
        decode_response(OpCode.PUBLISH, b"")
    with pytest.raises(ProtocolError, match="not a client request"):
        decode_request(OpCode.OK, b"")


RESPONSE_FIXTURES: list[tuple[str, m.Response, str]] = [
    ("Error", m.Error(404, "nope", None), "9401 0400 6e6f7065 00"),
    (
        "Error with detail",
        m.Error(503, "not leader", b'{"leader":"h:1"}'),
        "f701 0a00 6e6f74206c6561646572 01 10000000 7b226c6561646572223a22683a31227d",
    ),
    ("ConnectOk", m.ConnectOk("0.6.0", "n1", "h:5933"), "0500 302e362e30 0200 6e31 01 0600 683a35393333"),
    ("PublishOk", m.PublishOk(9, True), "0900000000000000 01"),
    (
        "PublishBatchOk",
        m.PublishBatchOk([(1, False), (1, True)]),
        "02000000 0100000000000000 00 0100000000000000 01",
    ),
    ("SubscribeOk", m.SubscribeOk(2), "02000000"),
    (
        "SubscriptionEnded",
        m.SubscriptionEnded(2, 404, "consumer deleted"),
        "02000000 9401 1000 636f6e73756d65722064656c65746564",
    ),
    (
        "Deliver",
        m.Deliver(2, [rec(2)]),
        """02000000 | 01000000
           36000000 06760cef 0200
           0200000000000000 02002a36fe9c9717
           0800 6f72646572732e32
           01 02000000 6b32
           02000000 0202
           0100 0100 68 0200 7632""",
    ),
    (
        "Messages",
        m.Messages([rec(0)]),
        """01000000
           34000000 b6f9c422 0000
           0000000000000000 00002a36fe9c9717
           0800 6f72646572732e30
           01 02000000 6b30
           00000000
           0100 0100 68 0200 7630""",
    ),
    (
        "ReadResult",
        m.ReadResult(10, 12, [rec(1)]),
        """0a00000000000000 0c00000000000000 01000000
           2f000000 f194cd5a 0100
           0100000000000000 01002a36fe9c9717
           0800 6f72646572732e31
           00
           01000000 01
           0100 0100 68 0200 7631""",
    ),
    (
        "CoreMsg",
        m.CoreMsg(0x80000001, "a", "r", [("h", "v")], b"x"),
        "01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78",
    ),
]


@pytest.mark.parametrize(("name", "resp", "data"), RESPONSE_FIXTURES, ids=[f[0] for f in RESPONSE_FIXTURES])
def test_response_decodes_from_the_rust_encoding(name: str, resp: m.Response, data: str) -> None:
    assert decode_response(resp.OPCODE, hx(data), verify_crc=True) == resp
    assert encode_response(resp).hex() == hx(data).hex()


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


def test_records_carry_a_crc32c_that_ignores_delivery_count() -> None:
    enc = bytearray(encode_record(rec(2)))
    assert len(enc) == 0x36 + 4
    assert verify_record_crc(enc)
    # The server patches delivery_count (bytes 8..10) in place.
    struct.pack_into("<H", enc, 8, 7)
    assert verify_record_crc(enc)
    resp = decode_response(OpCode.MESSAGES, hx("01000000") + bytes(enc), verify_crc=True)
    assert isinstance(resp, m.Messages)
    assert resp.records[0].delivery_count == 7
    enc[-1] ^= 1
    assert not verify_record_crc(enc)
    with pytest.raises(ProtocolError, match="CRC"):
        decode_response(OpCode.MESSAGES, hx("01000000") + bytes(enc), verify_crc=True)
    # Without verification the corrupted record still decodes.
    decode_response(OpCode.MESSAGES, hx("01000000") + bytes(enc))


def test_crc32c_matches_the_standard_check_value() -> None:
    assert crc32c(b"123456789") == 0xE3069283


def test_rejects_a_record_length_that_disagrees_with_its_contents() -> None:
    enc = bytearray(encode_record(rec(1)))
    struct.pack_into("<I", enc, 0, struct.unpack_from("<I", enc, 0)[0] + 1)
    with pytest.raises(ProtocolError):
        decode_response(OpCode.MESSAGES, hx("01000000") + bytes(enc) + hx("00"))
    with pytest.raises(ProtocolError, match="out of bounds"):
        decode_response(OpCode.MESSAGES, hx("01000000") + hx("01000000") + bytes(31))
