"""Constants of client protocol v2 (see ``docs/protocol.md``)."""

from __future__ import annotations

from enum import IntEnum

__all__ = [
    "PROTOCOL_VERSION",
    "FRAME_HEADER_SIZE",
    "MAX_PAYLOAD_SIZE",
    "DEFAULT_PORT",
    "MIN_RECORD_LEN",
    "MAX_RECORD_LEN",
    "OpCode",
    "ErrorCode",
    "SeekKind",
]

#: Wire protocol version spoken by this client (byte 0 of every frame).
PROTOCOL_VERSION = 0x02
#: ``[version u8][opcode u8][correlation id u32][payload length u32]``.
FRAME_HEADER_SIZE = 10
#: Largest payload either side accepts (16 MiB).
MAX_PAYLOAD_SIZE = 16 * 1024 * 1024
#: Default client port.
DEFAULT_PORT = 5933
#: Smallest encoded record: header + offset + timestamp + empty subject +
#: absent key + empty value + no headers.
MIN_RECORD_LEN = 10 + 8 + 8 + 2 + 1 + 4 + 2
#: Largest encoded record the server writes or accepts.
MAX_RECORD_LEN = 64 * 1024 * 1024


class OpCode(IntEnum):
    """Operation codes of client protocol v2."""

    # Requests (client -> server)
    CONNECT = 0x01
    METADATA = 0x03
    PUBLISH = 0x10
    PUBLISH_BATCH = 0x11
    CREATE_STREAM = 0x18
    UPDATE_STREAM = 0x19
    DELETE_STREAM = 0x1A
    STREAM_INFO = 0x1B
    LIST_STREAMS = 0x1C
    QUERY = 0x20
    CREATE_CONSUMER = 0x40
    DELETE_CONSUMER = 0x41
    CONSUMER_INFO = 0x42
    LIST_CONSUMERS = 0x43
    SEEK_CONSUMER = 0x44
    SUBSCRIBE = 0x50
    CREDIT = 0x51
    UNSUBSCRIBE = 0x52
    PULL = 0x53
    ACK = 0x54
    NACK = 0x55
    TERM = 0x56
    IN_PROGRESS = 0x57
    READ = 0x60
    CORE_PUBLISH = 0x70
    CORE_SUBSCRIBE = 0x71
    KV_PUT = 0x74
    KV_GET = 0x75
    KV_DELETE = 0x76
    KV_KEYS = 0x77
    KV_HISTORY = 0x78
    KV_CREATE_BUCKET = 0x79
    PING = 0xF0

    # Responses and pushes (server -> client)
    OK = 0x80
    ERROR = 0x81
    DELIVER = 0x82
    MESSAGES = 0x83
    READ_RESULT = 0x84
    JSON = 0x85
    PUBLISH_OK = 0x86
    PUBLISH_BATCH_OK = 0x87
    CONNECT_OK = 0x88
    SUBSCRIBE_OK = 0x89
    SUBSCRIPTION_ENDED = 0x8A
    CORE_MSG = 0x8B
    PONG = 0xF1


class ErrorCode(IntEnum):
    """Error codes the server returns (HTTP-like)."""

    #: Malformed request, invalid name or filter, invalid config.
    BAD_REQUEST = 400
    #: Not authenticated (bad or missing token).
    UNAUTHORIZED = 401
    #: The credential lacks the needed action on the stream.
    FORBIDDEN = 403
    #: Stream, consumer, bucket or key not found; or a request had no responders.
    NOT_FOUND = 404
    #: Exists with different settings, stream still has consumers, ``msg_id``
    #: reused with a different body, or a KV key not at the expected revision.
    CONFLICT = 409
    #: Retry later (dedup map full, stream full with ``discard="new"``, too
    #: many concurrent waiting requests).
    TOO_MANY_REQUESTS = 429
    #: Internal error.
    INTERNAL = 500
    #: Not the leader (see :attr:`ServerError.leader_hint`), or still starting.
    UNAVAILABLE = 503
    #: The server's disk is full; nothing was written.
    INSUFFICIENT_STORAGE = 507


class SeekKind(IntEnum):
    """Targets of ``SeekConsumer``."""

    EARLIEST = 0
    LATEST = 1
    OFFSET = 2
    #: Milliseconds since the Unix epoch.
    TIME = 3
