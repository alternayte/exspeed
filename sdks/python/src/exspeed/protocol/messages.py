"""Every request and response of client protocol v2 with its binary encoding.

This mirrors ``Request`` / ``Response`` in
``crates/exspeed-protocol/src/client.rs`` in both directions; the server side
(decoding requests, encoding responses) is what the unit tests' fake server
uses.

Each message is a dataclass with an ``OPCODE`` and ``encode`` / ``decode``
methods. Use :func:`request_frame`, :func:`decode_request`,
:func:`response_frame` and :func:`decode_response` rather than calling them
directly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, ClassVar, Union

from ..errors import ProtocolError
from .codec import Headers, Reader, WireRecord, Writer, encode_frame
from .constants import OpCode

__all__ = [
    "WirePublishRecord",
    "WireStreamSpec",
    "Request",
    "Response",
    "request_frame",
    "encode_request",
    "decode_request",
    "response_frame",
    "encode_response",
    "decode_response",
    "json_bytes",
]


def json_bytes(value: Any) -> bytes:
    """Compact JSON, as ``serde_json`` and ``JSON.stringify`` write it."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Shared structures
# ---------------------------------------------------------------------------


@dataclass
class WirePublishRecord:
    """``PublishRecord``: str subject, opt<bytes> key, bytes value, headers, opt<str> msg_id."""

    subject: str
    key: bytes | None
    value: bytes
    headers: Headers = field(default_factory=list)
    msg_id: str | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.subject).optional(self.key, Writer.byte_string).byte_string(self.value)
        w.headers(self.headers).optional(self.msg_id, Writer.string)

    @classmethod
    def decode(cls, r: Reader) -> WirePublishRecord:
        """Read the payload from ``r``."""
        return cls(
            subject=r.string(),
            key=r.optional(Reader.byte_string),
            value=r.byte_string(),
            headers=r.headers(),
            msg_id=r.optional(Reader.string),
        )


@dataclass
class WireStreamSpec:
    """``StreamSpec``: str name, four u64 settings (0 = server default), u8
    compaction, then ``bytes`` holding the limits JSON only when a limit isn't
    at its default (so older servers keep accepting the request)."""

    name: str
    max_age_secs: int = 0
    max_bytes: int = 0
    dedup_window_secs: int = 0
    dedup_max_entries: int = 0
    compaction: bool = False
    #: The ``StreamLimits`` JSON object (snake_case keys in Rust declaration
    #: order), or ``None`` when every limit is at its default.
    limits: dict[str, Any] | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.name).u64(self.max_age_secs).u64(self.max_bytes)
        w.u64(self.dedup_window_secs).u64(self.dedup_max_entries).u8(1 if self.compaction else 0)
        if self.limits is not None:
            w.byte_string(json_bytes(self.limits))

    @classmethod
    def decode(cls, r: Reader) -> WireStreamSpec:
        """Read the payload from ``r``."""
        spec = cls(
            name=r.string(),
            max_age_secs=r.u64(),
            max_bytes=r.u64(),
            dedup_window_secs=r.u64(),
            dedup_max_entries=r.u64(),
            compaction=r.u8() != 0,
        )
        if r.remaining > 0:
            raw = r.byte_string()
            try:
                limits = json.loads(raw)
            except ValueError as e:
                raise ProtocolError(f"invalid stream limits: {e}") from None
            if not isinstance(limits, dict):
                raise ProtocolError("invalid stream limits: not an object")
            spec.limits = limits
        return spec


def _write_offsets(w: Writer, offsets: list[int]) -> None:
    w.u32(len(offsets))
    for o in offsets:
        w.u64(o)


def _read_offsets(r: Reader) -> list[int]:
    return [r.u64() for _ in range(r.count(8))]


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


@dataclass
class Connect:
    """``Connect``: str client_id, opt<str> token. Must be the first frame."""

    OPCODE: ClassVar[OpCode] = OpCode.CONNECT
    client_id: str
    token: str | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.client_id).optional(self.token, Writer.string)

    @classmethod
    def decode(cls, r: Reader) -> Connect:
        """Read the payload from ``r``."""
        return cls(r.string(), r.optional(Reader.string))


@dataclass
class Ping:
    """``Ping``: no payload; answered with ``Pong``."""

    OPCODE: ClassVar[OpCode] = OpCode.PING

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w`` (it has none)."""

    @classmethod
    def decode(cls, r: Reader) -> Ping:
        """Read the payload from ``r``."""
        return cls()


@dataclass
class Metadata:
    """``Metadata``: no payload; answered with ``Json``."""

    OPCODE: ClassVar[OpCode] = OpCode.METADATA

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w`` (it has none)."""

    @classmethod
    def decode(cls, r: Reader) -> Metadata:
        """Read the payload from ``r``."""
        return cls()


@dataclass
class Publish:
    """``Publish``: str stream, PublishRecord."""

    OPCODE: ClassVar[OpCode] = OpCode.PUBLISH
    stream: str
    record: WirePublishRecord

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.stream)
        self.record.encode(w)

    @classmethod
    def decode(cls, r: Reader) -> Publish:
        """Read the payload from ``r``."""
        return cls(r.string(), WirePublishRecord.decode(r))


@dataclass
class PublishBatch:
    """``PublishBatch``: str stream, vec<PublishRecord>."""

    OPCODE: ClassVar[OpCode] = OpCode.PUBLISH_BATCH
    stream: str
    records: list[WirePublishRecord]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.stream).u32(len(self.records))
        for rec in self.records:
            rec.encode(w)

    @classmethod
    def decode(cls, r: Reader) -> PublishBatch:
        """Read the payload from ``r``."""
        stream = r.string()
        n = r.count(10)
        return cls(stream, [WirePublishRecord.decode(r) for _ in range(n)])


@dataclass
class CreateStream:
    """``CreateStream``: StreamSpec."""

    OPCODE: ClassVar[OpCode] = OpCode.CREATE_STREAM
    spec: WireStreamSpec

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        self.spec.encode(w)

    @classmethod
    def decode(cls, r: Reader) -> CreateStream:
        """Read the payload from ``r``."""
        return cls(WireStreamSpec.decode(r))


@dataclass
class UpdateStream:
    """``UpdateStream``: StreamSpec."""

    OPCODE: ClassVar[OpCode] = OpCode.UPDATE_STREAM
    spec: WireStreamSpec

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        self.spec.encode(w)

    @classmethod
    def decode(cls, r: Reader) -> UpdateStream:
        """Read the payload from ``r``."""
        return cls(WireStreamSpec.decode(r))


@dataclass
class DeleteStream:
    """``DeleteStream``: str name."""

    OPCODE: ClassVar[OpCode] = OpCode.DELETE_STREAM
    name: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.name)

    @classmethod
    def decode(cls, r: Reader) -> DeleteStream:
        """Read the payload from ``r``."""
        return cls(r.string())


@dataclass
class StreamInfo:
    """``StreamInfo``: str name; answered with ``Json``."""

    OPCODE: ClassVar[OpCode] = OpCode.STREAM_INFO
    name: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.name)

    @classmethod
    def decode(cls, r: Reader) -> StreamInfo:
        """Read the payload from ``r``."""
        return cls(r.string())


@dataclass
class ListStreams:
    """``ListStreams``: no payload; answered with a ``Json`` array."""

    OPCODE: ClassVar[OpCode] = OpCode.LIST_STREAMS

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w`` (it has none)."""

    @classmethod
    def decode(cls, r: Reader) -> ListStreams:
        """Read the payload from ``r``."""
        return cls()


@dataclass
class Query:
    """``Query``: lstr sql; answered with ``Json``."""

    OPCODE: ClassVar[OpCode] = OpCode.QUERY
    sql: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.long_string(self.sql)

    @classmethod
    def decode(cls, r: Reader) -> Query:
        """Read the payload from ``r``."""
        return cls(r.long_string())


@dataclass
class CreateConsumer:
    """``CreateConsumer``: bytes holding the ConsumerSpec JSON object (snake_case)."""

    OPCODE: ClassVar[OpCode] = OpCode.CREATE_CONSUMER
    spec: dict[str, Any]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.byte_string(json_bytes(self.spec))

    @classmethod
    def decode(cls, r: Reader) -> CreateConsumer:
        """Read the payload from ``r``."""
        raw = r.byte_string()
        try:
            spec = json.loads(raw)
        except ValueError as e:
            raise ProtocolError(f"invalid consumer spec: {e}") from None
        if not isinstance(spec, dict):
            raise ProtocolError("invalid consumer spec: not an object")
        return cls(spec)


@dataclass
class DeleteConsumer:
    """``DeleteConsumer``: str name."""

    OPCODE: ClassVar[OpCode] = OpCode.DELETE_CONSUMER
    name: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.name)

    @classmethod
    def decode(cls, r: Reader) -> DeleteConsumer:
        """Read the payload from ``r``."""
        return cls(r.string())


@dataclass
class ConsumerInfo:
    """``ConsumerInfo``: str name; answered with ``Json``."""

    OPCODE: ClassVar[OpCode] = OpCode.CONSUMER_INFO
    name: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.name)

    @classmethod
    def decode(cls, r: Reader) -> ConsumerInfo:
        """Read the payload from ``r``."""
        return cls(r.string())


@dataclass
class ListConsumers:
    """``ListConsumers``: opt<str> stream; answered with a ``Json`` array."""

    OPCODE: ClassVar[OpCode] = OpCode.LIST_CONSUMERS
    stream: str | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.optional(self.stream, Writer.string)

    @classmethod
    def decode(cls, r: Reader) -> ListConsumers:
        """Read the payload from ``r``."""
        return cls(r.optional(Reader.string))


@dataclass
class SeekConsumer:
    """``SeekConsumer``: str consumer, u8 kind (0 earliest, 1 latest, 2 offset, 3 time ms), u64 value."""

    OPCODE: ClassVar[OpCode] = OpCode.SEEK_CONSUMER
    consumer: str
    kind: int
    value: int = 0

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer).u8(self.kind).u64(self.value)

    @classmethod
    def decode(cls, r: Reader) -> SeekConsumer:
        """Read the payload from ``r``."""
        consumer, kind, value = r.string(), r.u8(), r.u64()
        if kind > 3:
            raise ProtocolError(f"unknown seek kind {kind}")
        return cls(consumer, kind, value)


@dataclass
class Subscribe:
    """``Subscribe``: str consumer, u32 credits; answered with ``SubscribeOk``, then ``Deliver`` pushes."""

    OPCODE: ClassVar[OpCode] = OpCode.SUBSCRIBE
    consumer: str
    credits: int

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer).u32(self.credits)

    @classmethod
    def decode(cls, r: Reader) -> Subscribe:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u32())


@dataclass
class Credit:
    """``Credit``: u32 sub_id, u32 credits."""

    OPCODE: ClassVar[OpCode] = OpCode.CREDIT
    sub_id: int
    credits: int

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id).u32(self.credits)

    @classmethod
    def decode(cls, r: Reader) -> Credit:
        """Read the payload from ``r``."""
        return cls(r.u32(), r.u32())


@dataclass
class Unsubscribe:
    """``Unsubscribe``: u32 sub_id (consumer or core subscription)."""

    OPCODE: ClassVar[OpCode] = OpCode.UNSUBSCRIBE
    sub_id: int

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id)

    @classmethod
    def decode(cls, r: Reader) -> Unsubscribe:
        """Read the payload from ``r``."""
        return cls(r.u32())


@dataclass
class Pull:
    """``Pull``: str consumer, u32 max_messages, u32 max_bytes, u32 expires_ms; answered with ``Messages``."""

    OPCODE: ClassVar[OpCode] = OpCode.PULL
    consumer: str
    max_messages: int
    max_bytes: int
    expires_ms: int

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer).u32(self.max_messages).u32(self.max_bytes).u32(self.expires_ms)

    @classmethod
    def decode(cls, r: Reader) -> Pull:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u32(), r.u32(), r.u32())


@dataclass
class Ack:
    """``Ack``: str consumer, vec<u64> offsets."""

    OPCODE: ClassVar[OpCode] = OpCode.ACK
    consumer: str
    offsets: list[int]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer)
        _write_offsets(w, self.offsets)

    @classmethod
    def decode(cls, r: Reader) -> Ack:
        """Read the payload from ``r``."""
        return cls(r.string(), _read_offsets(r))


@dataclass
class Nack:
    """``Nack``: str consumer, u64 offset, u32 delay_ms (0 = the consumer's backoff)."""

    OPCODE: ClassVar[OpCode] = OpCode.NACK
    consumer: str
    offset: int
    delay_ms: int = 0

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer).u64(self.offset).u32(self.delay_ms)

    @classmethod
    def decode(cls, r: Reader) -> Nack:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u64(), r.u32())


@dataclass
class Term:
    """``Term``: str consumer, u64 offset, str reason (dead-letters now)."""

    OPCODE: ClassVar[OpCode] = OpCode.TERM
    consumer: str
    offset: int
    reason: str = ""

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer).u64(self.offset).string(self.reason)

    @classmethod
    def decode(cls, r: Reader) -> Term:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u64(), r.string())


@dataclass
class InProgress:
    """``InProgress``: str consumer, vec<u64> offsets (resets the ack timers)."""

    OPCODE: ClassVar[OpCode] = OpCode.IN_PROGRESS
    consumer: str
    offsets: list[int]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.consumer)
        _write_offsets(w, self.offsets)

    @classmethod
    def decode(cls, r: Reader) -> InProgress:
        """Read the payload from ``r``."""
        return cls(r.string(), _read_offsets(r))


@dataclass
class Read:
    """``Read``: str stream, u64 from, u32 max_records, u32 max_bytes, u32 wait_ms, str filter."""

    OPCODE: ClassVar[OpCode] = OpCode.READ
    stream: str
    from_offset: int
    max_records: int
    max_bytes: int
    wait_ms: int
    filter: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.stream).u64(self.from_offset).u32(self.max_records).u32(self.max_bytes)
        w.u32(self.wait_ms).string(self.filter)

    @classmethod
    def decode(cls, r: Reader) -> Read:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u64(), r.u32(), r.u32(), r.u32(), r.string())


@dataclass
class CorePublish:
    """``CorePublish``: str subject, opt<str> reply_to, headers, bytes value."""

    OPCODE: ClassVar[OpCode] = OpCode.CORE_PUBLISH
    subject: str
    reply_to: str | None
    headers: Headers
    value: bytes

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.subject).optional(self.reply_to, Writer.string).headers(self.headers)
        w.byte_string(self.value)

    @classmethod
    def decode(cls, r: Reader) -> CorePublish:
        """Read the payload from ``r``."""
        return cls(r.string(), r.optional(Reader.string), r.headers(), r.byte_string())


@dataclass
class CoreSubscribe:
    """``CoreSubscribe``: str subject_filter, opt<str> queue_group."""

    OPCODE: ClassVar[OpCode] = OpCode.CORE_SUBSCRIBE
    subject: str
    queue: str | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.subject).optional(self.queue, Writer.string)

    @classmethod
    def decode(cls, r: Reader) -> CoreSubscribe:
        """Read the payload from ``r``."""
        return cls(r.string(), r.optional(Reader.string))


@dataclass
class KvCreateBucket:
    """``KvCreateBucket``: str bucket, u64 history, u64 ttl_ms, u64 max_bytes."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_CREATE_BUCKET
    bucket: str
    history: int = 0
    ttl_ms: int = 0
    max_bytes: int = 0

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).u64(self.history).u64(self.ttl_ms).u64(self.max_bytes)

    @classmethod
    def decode(cls, r: Reader) -> KvCreateBucket:
        """Read the payload from ``r``."""
        return cls(r.string(), r.u64(), r.u64(), r.u64())


@dataclass
class KvPut:
    """``KvPut``: str bucket, str key, bytes value, opt<u64> expected_revision, opt<u64> ttl_ms."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_PUT
    bucket: str
    key: str
    value: bytes
    expected_revision: int | None = None
    ttl_ms: int | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).string(self.key).byte_string(self.value)
        w.optional(self.expected_revision, Writer.u64).optional(self.ttl_ms, Writer.u64)

    @classmethod
    def decode(cls, r: Reader) -> KvPut:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string(), r.byte_string(), r.optional(Reader.u64), r.optional(Reader.u64))


@dataclass
class KvGet:
    """``KvGet``: str bucket, str key, opt<u64> revision."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_GET
    bucket: str
    key: str
    revision: int | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).string(self.key).optional(self.revision, Writer.u64)

    @classmethod
    def decode(cls, r: Reader) -> KvGet:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string(), r.optional(Reader.u64))


@dataclass
class KvDelete:
    """``KvDelete``: str bucket, str key, u8 purge, opt<u64> expected_revision."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_DELETE
    bucket: str
    key: str
    purge: bool = False
    expected_revision: int | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).string(self.key).u8(1 if self.purge else 0)
        w.optional(self.expected_revision, Writer.u64)

    @classmethod
    def decode(cls, r: Reader) -> KvDelete:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string(), r.u8() != 0, r.optional(Reader.u64))


@dataclass
class KvKeys:
    """``KvKeys``: str bucket, str filter; answered with a ``Json`` array of keys."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_KEYS
    bucket: str
    filter: str = ""

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).string(self.filter)

    @classmethod
    def decode(cls, r: Reader) -> KvKeys:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string())


@dataclass
class KvHistory:
    """``KvHistory``: str bucket, str key; answered with ``Messages``."""

    OPCODE: ClassVar[OpCode] = OpCode.KV_HISTORY
    bucket: str
    key: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.bucket).string(self.key)

    @classmethod
    def decode(cls, r: Reader) -> KvHistory:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string())


Request = Union[
    Connect,
    Ping,
    Metadata,
    Publish,
    PublishBatch,
    CreateStream,
    UpdateStream,
    DeleteStream,
    StreamInfo,
    ListStreams,
    Query,
    CreateConsumer,
    DeleteConsumer,
    ConsumerInfo,
    ListConsumers,
    SeekConsumer,
    Subscribe,
    Credit,
    Unsubscribe,
    Pull,
    Ack,
    Nack,
    Term,
    InProgress,
    Read,
    CorePublish,
    CoreSubscribe,
    KvCreateBucket,
    KvPut,
    KvGet,
    KvDelete,
    KvKeys,
    KvHistory,
]
"""Any client request."""

_REQUEST_TYPES: dict[int, Any] = {
    t.OPCODE: t
    for t in (
        Connect, Ping, Metadata, Publish, PublishBatch, CreateStream, UpdateStream, DeleteStream,
        StreamInfo, ListStreams, Query, CreateConsumer, DeleteConsumer, ConsumerInfo, ListConsumers,
        SeekConsumer, Subscribe, Credit, Unsubscribe, Pull, Ack, Nack, Term, InProgress, Read,
        CorePublish, CoreSubscribe, KvCreateBucket, KvPut, KvGet, KvDelete, KvKeys, KvHistory,
    )
}  # fmt: skip


def encode_request(req: Request) -> bytes:
    """The payload of a request."""
    w = Writer()
    req.encode(w)
    return w.finish()


def request_frame(req: Request, correlation_id: int) -> bytes:
    """A complete request frame (header included)."""
    return encode_frame(req.OPCODE, correlation_id, encode_request(req))


def decode_request(opcode: int, payload: bytes) -> Request:
    """Decode a request payload (used by test servers)."""
    cls = _REQUEST_TYPES.get(opcode)
    if cls is None:
        raise ProtocolError(f"opcode {opcode:#04x} is not a client request")
    r = Reader(payload)
    req: Request = cls.decode(r)
    r.finish()
    return req


# ---------------------------------------------------------------------------
# Responses and pushes
# ---------------------------------------------------------------------------


@dataclass
class Ok:
    """``Ok``: no payload."""

    OPCODE: ClassVar[OpCode] = OpCode.OK

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w`` (it has none)."""

    @classmethod
    def decode(cls, r: Reader) -> Ok:
        """Read the payload from ``r``."""
        return cls()


@dataclass
class Pong:
    """``Pong``: no payload."""

    OPCODE: ClassVar[OpCode] = OpCode.PONG

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w`` (it has none)."""

    @classmethod
    def decode(cls, r: Reader) -> Pong:
        """Read the payload from ``r``."""
        return cls()


@dataclass
class Error:
    """``Error``: u16 code, str message, opt<bytes> detail (JSON)."""

    OPCODE: ClassVar[OpCode] = OpCode.ERROR
    code: int
    message: str
    detail: bytes | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u16(self.code).string(self.message).optional(self.detail, Writer.byte_string)

    @classmethod
    def decode(cls, r: Reader) -> Error:
        """Read the payload from ``r``."""
        return cls(r.u16(), r.string(), r.optional(Reader.byte_string))


@dataclass
class ConnectOk:
    """``ConnectOk``: str server_version, str node_id, opt<str> leader."""

    OPCODE: ClassVar[OpCode] = OpCode.CONNECT_OK
    server_version: str
    node_id: str
    leader: str | None = None

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.string(self.server_version).string(self.node_id).optional(self.leader, Writer.string)

    @classmethod
    def decode(cls, r: Reader) -> ConnectOk:
        """Read the payload from ``r``."""
        return cls(r.string(), r.string(), r.optional(Reader.string))


@dataclass
class PublishOk:
    """``PublishOk``: u64 offset, u8 duplicate."""

    OPCODE: ClassVar[OpCode] = OpCode.PUBLISH_OK
    offset: int
    duplicate: bool = False

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u64(self.offset).u8(1 if self.duplicate else 0)

    @classmethod
    def decode(cls, r: Reader) -> PublishOk:
        """Read the payload from ``r``."""
        return cls(r.u64(), r.u8() != 0)


@dataclass
class PublishBatchOk:
    """``PublishBatchOk``: vec<(u64 offset, u8 duplicate)>."""

    OPCODE: ClassVar[OpCode] = OpCode.PUBLISH_BATCH_OK
    results: list[tuple[int, bool]]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(len(self.results))
        for offset, dup in self.results:
            w.u64(offset).u8(1 if dup else 0)

    @classmethod
    def decode(cls, r: Reader) -> PublishBatchOk:
        """Read the payload from ``r``."""
        n = r.count(9)
        return cls([(r.u64(), r.u8() != 0) for _ in range(n)])


@dataclass
class SubscribeOk:
    """``SubscribeOk``: u32 sub_id (core subscriptions have the high bit set)."""

    OPCODE: ClassVar[OpCode] = OpCode.SUBSCRIBE_OK
    sub_id: int

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id)

    @classmethod
    def decode(cls, r: Reader) -> SubscribeOk:
        """Read the payload from ``r``."""
        return cls(r.u32())


@dataclass
class Deliver:
    """``Deliver`` push: u32 sub_id, vec<WireRecord>."""

    OPCODE: ClassVar[OpCode] = OpCode.DELIVER
    sub_id: int
    records: list[WireRecord]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id).records(self.records)

    @classmethod
    def decode(cls, r: Reader) -> Deliver:
        """Read the payload from ``r``."""
        return cls(r.u32(), r.records())


@dataclass
class SubscriptionEnded:
    """``SubscriptionEnded`` push: u32 sub_id, u16 code, str message."""

    OPCODE: ClassVar[OpCode] = OpCode.SUBSCRIPTION_ENDED
    sub_id: int
    code: int
    message: str

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id).u16(self.code).string(self.message)

    @classmethod
    def decode(cls, r: Reader) -> SubscriptionEnded:
        """Read the payload from ``r``."""
        return cls(r.u32(), r.u16(), r.string())


@dataclass
class Messages:
    """``Messages``: vec<WireRecord> (reply to Pull, KvGet, KvHistory)."""

    OPCODE: ClassVar[OpCode] = OpCode.MESSAGES
    records: list[WireRecord]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.records(self.records)

    @classmethod
    def decode(cls, r: Reader) -> Messages:
        """Read the payload from ``r``."""
        return cls(r.records())


@dataclass
class ReadResult:
    """``ReadResult``: u64 next_offset, u64 high_watermark, vec<WireRecord>."""

    OPCODE: ClassVar[OpCode] = OpCode.READ_RESULT
    next_offset: int
    high_watermark: int
    records: list[WireRecord]

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u64(self.next_offset).u64(self.high_watermark).records(self.records)

    @classmethod
    def decode(cls, r: Reader) -> ReadResult:
        """Read the payload from ``r``."""
        return cls(r.u64(), r.u64(), r.records())


@dataclass
class Json:
    """``Json``: raw UTF-8 JSON."""

    OPCODE: ClassVar[OpCode] = OpCode.JSON
    data: bytes

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.raw(self.data)

    @classmethod
    def decode(cls, r: Reader) -> Json:
        """Read the payload from ``r``."""
        return cls(bytes(r.take(r.remaining)))


@dataclass
class CoreMsg:
    """``CoreMsg`` push: u32 sub_id, str subject, opt<str> reply_to, headers, bytes value."""

    OPCODE: ClassVar[OpCode] = OpCode.CORE_MSG
    sub_id: int
    subject: str
    reply_to: str | None
    headers: Headers
    value: bytes

    def encode(self, w: Writer) -> None:
        """Write this message's payload to ``w``."""
        w.u32(self.sub_id).string(self.subject).optional(self.reply_to, Writer.string)
        w.headers(self.headers).byte_string(self.value)

    @classmethod
    def decode(cls, r: Reader) -> CoreMsg:
        """Read the payload from ``r``."""
        return cls(r.u32(), r.string(), r.optional(Reader.string), r.headers(), r.byte_string())


Response = Union[
    Ok,
    Pong,
    Error,
    ConnectOk,
    PublishOk,
    PublishBatchOk,
    SubscribeOk,
    Deliver,
    SubscriptionEnded,
    Messages,
    ReadResult,
    Json,
    CoreMsg,
]
"""Any server response or push."""

_RESPONSE_TYPES: dict[int, Any] = {
    t.OPCODE: t
    for t in (
        Ok, Pong, Error, ConnectOk, PublishOk, PublishBatchOk, SubscribeOk, Deliver,
        SubscriptionEnded, Messages, ReadResult, Json, CoreMsg,
    )
}  # fmt: skip


def encode_response(resp: Response) -> bytes:
    """The payload of a response (used by test servers)."""
    w = Writer()
    resp.encode(w)
    return w.finish()


def response_frame(resp: Response, correlation_id: int) -> bytes:
    """A complete response frame (used by test servers)."""
    return encode_frame(resp.OPCODE, correlation_id, encode_response(resp))


def decode_response(opcode: int, payload: bytes, *, verify_crc: bool = False) -> Response:
    """Decode a response or push payload."""
    cls = _RESPONSE_TYPES.get(opcode)
    if cls is None:
        raise ProtocolError(f"opcode {opcode:#04x} is not a server response")
    r = Reader(payload, verify_crc=verify_crc)
    resp: Response = cls.decode(r)
    r.finish()
    return resp
