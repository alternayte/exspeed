"""Public data types: stream and consumer specs, publish records, results,
info objects and client options.

Units: client-side durations (timeouts, ``ttl``, ``delay``, ``wait``,
``expires``) are seconds as ``float`` or a :class:`datetime.timedelta`.
Settings whose name ends in ``_ms`` are integer milliseconds, as on the wire.
"""

from __future__ import annotations

import json
import math
import re
import ssl
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

from .errors import ExspeedError, ProtocolError
from .protocol.codec import Headers
from .protocol.messages import WirePublishRecord, WireStreamSpec, json_bytes

__all__ = [
    "Value",
    "HeadersInit",
    "Duration",
    "TTL_HEADER",
    "DELAY_HEADER",
    "DELIVER_AT_HEADER",
    "PRIORITY_HEADER",
    "MSG_ID_HEADER",
    "StreamSpec",
    "StreamConfig",
    "StreamInfo",
    "PublishRecord",
    "PublishResult",
    "DeliverFromOffset",
    "DeliverFromTime",
    "DeliverPolicy",
    "ConsumerSpec",
    "ConsumerStats",
    "ConsumerInfo",
    "SeekTime",
    "SeekTarget",
    "ReconnectOptions",
    "TlsOptions",
    "ServerInfo",
    "Metadata",
    "QueryResult",
]

#: A record or message value: ``bytes`` / ``bytearray`` / ``memoryview`` are
#: sent as-is, ``str`` as UTF-8, anything else is JSON-encoded.
Value = Any

#: Headers: a mapping, or ``(key, value)`` pairs (which may repeat a key).
HeadersInit = Mapping[str, str] | Iterable[tuple[str, str]] | None

#: A duration: seconds (``float`` or ``int``) or a :class:`datetime.timedelta`.
Duration = float | int | timedelta

#: Header behind the ``ttl`` publish option.
TTL_HEADER = "exspeed-ttl"
#: Header behind the ``delay`` publish option.
DELAY_HEADER = "exspeed-delay"
#: Header behind the ``deliver_at`` publish option (ms since the epoch).
DELIVER_AT_HEADER = "exspeed-deliver-at"
#: Header behind the ``priority`` publish option (0 to 9).
PRIORITY_HEADER = "exspeed-priority"
#: Header the server stores a record's ``msg_id`` under.
MSG_ID_HEADER = "x-idempotency-key"

_DURATION = re.compile(r"^\d+\s*(ms|s|m|h|d)?$")


def encode_value(value: Value) -> bytes:
    """Encode a value: bytes as-is, ``str`` as UTF-8, anything else as compact JSON."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    try:
        return json_bytes(value)
    except (TypeError, ValueError) as e:
        raise ExspeedError(f"value is not JSON-serializable: {e}") from None


def to_headers(h: HeadersInit) -> Headers:
    """Normalize headers to a list of ``(key, value)`` string pairs."""
    if h is None:
        return []
    if isinstance(h, Mapping):
        return [(str(k), str(v)) for k, v in h.items()]
    return [(str(k), str(v)) for k, v in h]


def duration_seconds(v: Duration, name: str) -> float:
    """Seconds of a duration, validated."""
    if isinstance(v, timedelta):
        secs = v.total_seconds()
    elif isinstance(v, (int, float)) and not isinstance(v, bool):
        secs = float(v)
    else:
        raise ExspeedError(f"{name} must be a number of seconds or a timedelta, got {v!r}")
    if not math.isfinite(secs) or secs < 0:
        raise ExspeedError(f"{name} must be a non-negative duration, got {v!r}")
    return secs


def duration_ms(v: Duration, name: str, minimum: int = 0) -> int:
    """Whole milliseconds of a duration, rounded up, at least ``minimum``."""
    secs = duration_seconds(v, name)
    return max(minimum, math.ceil(round(secs * 1000, 6)))


def _duration_header(name: str, v: Duration | str, minimum: int) -> str:
    if isinstance(v, str):
        t = v.strip()
        if not _DURATION.match(t):
            raise ExspeedError(f"invalid {name} {v!r}: expected a number with an optional unit (ms, s, m, h, d)")
        return t
    return f"{duration_ms(v, name, minimum)}ms"


def epoch_ms(v: datetime | float | int, name: str) -> int:
    """Milliseconds since the Unix epoch of a datetime or a Unix timestamp in seconds."""
    if isinstance(v, datetime):
        secs = v.timestamp()
    elif isinstance(v, (int, float)) and not isinstance(v, bool):
        secs = float(v)
    else:
        raise ExspeedError(f"invalid {name}: {v!r}")
    if not math.isfinite(secs) or secs < 0:
        raise ExspeedError(f"invalid {name}: {v!r}")
    return round(secs * 1000)


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


@dataclass
class StreamSpec:
    """Stream settings. ``0`` numeric settings mean "server default"."""

    name: str
    #: Retention by age (seconds).
    max_age_secs: int = 0
    #: Retention by size (bytes).
    max_bytes: int = 0
    #: How long ``msg_id``s are remembered for deduplication (seconds).
    dedup_window_secs: int = 0
    #: Most ``msg_id``s remembered.
    dedup_max_entries: int = 0
    #: Keep only the latest record per key (log compaction).
    compaction: bool = False
    #: Most records the stream holds; 0 = no limit. What happens at the limit is ``discard``.
    max_msgs: int = 0
    #: At ``max_msgs``: ``"old"`` drops the oldest records, ``"new"`` rejects new ones (429).
    discard: Literal["old", "new"] = "old"
    #: Most records kept per subject (older ones are removed); 0 = no limit.
    max_msgs_per_subject: int = 0
    #: Accept the per-record ``ttl`` publish option (header ``exspeed-ttl``).
    allow_msg_ttl: bool = False
    #: Default lifetime of every record, in ms; 0 = none.
    msg_ttl_ms: int = 0
    #: Accept the ``delay`` / ``deliver_at`` publish options.
    allow_delayed: bool = False
    #: ``"limits"``: records stay until a limit removes them. ``"work_queue"``:
    #: at most one consumer; a record is removed once acked. ``"interest"``: a
    #: record is removed once every consumer acked it.
    retention: Literal["limits", "work_queue", "interest"] = "limits"

    def to_wire(self) -> WireStreamSpec:
        """The wire form, with the limits trailer only when a limit isn't at its default."""
        if not self.name:
            raise ExspeedError("stream name is required")
        limits: dict[str, Any] = {
            "max_msgs": self.max_msgs,
            "discard": self.discard,
            "max_msgs_per_subject": self.max_msgs_per_subject,
            "allow_msg_ttl": self.allow_msg_ttl,
            "msg_ttl_ms": self.msg_ttl_ms,
            "allow_delayed": self.allow_delayed,
            "retention": self.retention,
        }
        default = (
            self.max_msgs == 0
            and self.discard == "old"
            and self.max_msgs_per_subject == 0
            and not self.allow_msg_ttl
            and self.msg_ttl_ms == 0
            and not self.allow_delayed
            and self.retention == "limits"
        )
        return WireStreamSpec(
            name=self.name,
            max_age_secs=self.max_age_secs,
            max_bytes=self.max_bytes,
            dedup_window_secs=self.dedup_window_secs,
            dedup_max_entries=self.dedup_max_entries,
            compaction=self.compaction,
            limits=None if default else limits,
        )


def _int(d: Mapping[str, Any], key: str, default: int = 0) -> int:
    v = d.get(key, default)
    return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else default


@dataclass
class StreamConfig:
    """A stream's settings as the server reports them."""

    max_age_secs: int
    max_bytes: int
    dedup_window_secs: int
    dedup_max_entries: int
    compaction: bool
    max_msgs: int
    discard: str
    max_msgs_per_subject: int
    allow_msg_ttl: bool
    msg_ttl_ms: int
    allow_delayed: bool
    retention: str
    #: The JSON object as received (includes settings this client doesn't model).
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> StreamConfig:
        """Build from the server's JSON."""
        return cls(
            max_age_secs=_int(d, "max_age_secs"),
            max_bytes=_int(d, "max_bytes"),
            dedup_window_secs=_int(d, "dedup_window_secs"),
            dedup_max_entries=_int(d, "dedup_max_entries"),
            compaction=bool(d.get("compaction", False)),
            max_msgs=_int(d, "max_msgs"),
            discard=str(d.get("discard", "old")),
            max_msgs_per_subject=_int(d, "max_msgs_per_subject"),
            allow_msg_ttl=bool(d.get("allow_msg_ttl", False)),
            msg_ttl_ms=_int(d, "msg_ttl_ms"),
            allow_delayed=bool(d.get("allow_delayed", False)),
            retention=str(d.get("retention", "limits")),
            raw=dict(d),
        )


@dataclass
class StreamInfo:
    """A stream's bounds and settings."""

    name: str
    #: First retained offset.
    earliest_offset: int
    #: The offset the next record gets.
    next_offset: int
    #: Records between the two.
    records: int
    config: StreamConfig
    #: Internal streams' names start with ``__``.
    internal: bool
    #: The JSON object as received.
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> StreamInfo:
        """Build from the server's JSON."""
        cfg = d.get("config")
        return cls(
            name=str(d.get("name", "")),
            earliest_offset=_int(d, "earliest_offset"),
            next_offset=_int(d, "next_offset"),
            records=_int(d, "records"),
            config=StreamConfig.from_json(cfg if isinstance(cfg, Mapping) else {}),
            internal=bool(d.get("internal", False)),
            raw=dict(d),
        )


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


@dataclass
class PublishRecord:
    """One record to publish (for :meth:`ExspeedClient.publish_batch`).

    The time and priority options become headers: ``ttl`` -> ``exspeed-ttl``,
    ``delay`` -> ``exspeed-delay``, ``deliver_at`` -> ``exspeed-deliver-at``,
    ``priority`` -> ``exspeed-priority``.
    """

    subject: str
    #: Bytes are sent as-is, ``str`` as UTF-8, anything else as JSON.
    value: Value = b""
    #: Partition/compaction key (bytes or UTF-8 text).
    key: bytes | str | None = None
    headers: HeadersInit = None
    #: Idempotency key: a retry with the same ``msg_id`` and body returns the
    #: original offset with ``duplicate=True`` instead of writing again.
    msg_id: str | None = None
    #: Expire the record this long after the append: seconds, a timedelta, or
    #: a duration string such as ``"30s"`` (units ms, s, m, h, d). The stream
    #: needs ``allow_msg_ttl``.
    ttl: Duration | str | None = None
    #: Deliver to consumers no earlier than this long after the append. The
    #: stream needs ``allow_delayed``.
    delay: Duration | str | None = None
    #: Deliver to consumers no earlier than this time: a datetime, or a Unix
    #: timestamp in seconds. The stream needs ``allow_delayed``.
    deliver_at: datetime | float | None = None
    #: 0 (default) to 9, higher first, for consumers with a ``priority_window``.
    priority: int | None = None

    def to_wire(self) -> WirePublishRecord:
        """The wire form, with the option headers appended after ``headers``."""
        if not isinstance(self.subject, str):
            raise ExspeedError("record subject is required")
        headers = to_headers(self.headers)
        if self.ttl is not None:
            headers.append((TTL_HEADER, _duration_header("ttl", self.ttl, 1)))
        if self.delay is not None:
            headers.append((DELAY_HEADER, _duration_header("delay", self.delay, 0)))
        if self.deliver_at is not None:
            headers.append((DELIVER_AT_HEADER, str(epoch_ms(self.deliver_at, "deliver_at"))))
        if self.priority is not None:
            p = self.priority
            if not isinstance(p, int) or isinstance(p, bool) or not 0 <= p <= 9:
                raise ExspeedError(f"priority must be an integer from 0 to 9, got {p!r}")
            headers.append((PRIORITY_HEADER, str(p)))
        return WirePublishRecord(
            subject=self.subject,
            key=None if self.key is None else encode_value(self.key),
            value=encode_value(self.value),
            headers=headers,
            msg_id=self.msg_id,
        )


@dataclass(frozen=True)
class PublishResult:
    """The outcome of one published record."""

    offset: int
    #: True when ``msg_id`` matched an earlier publish; nothing was written.
    duplicate: bool = False


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeliverFromOffset:
    """Start a new consumer at this offset."""

    offset: int


@dataclass(frozen=True)
class DeliverFromTime:
    """Start a new consumer at the first record at or after this time (a datetime, or ms since the epoch)."""

    time: datetime | int

    @property
    def time_ms(self) -> int:
        """The time in ms since the epoch."""
        if isinstance(self.time, datetime):
            return epoch_ms(self.time, "from_time")
        return int(self.time)


#: Where a new consumer starts: ``"all"``, ``"new"``, or a
#: :class:`DeliverFromOffset` / :class:`DeliverFromTime`.
DeliverPolicy = Literal["all", "new"] | DeliverFromOffset | DeliverFromTime


def _deliver_to_wire(d: DeliverPolicy) -> Any:
    if d in ("all", "new"):
        return d
    if isinstance(d, DeliverFromOffset):
        return {"from_offset": d.offset}
    if isinstance(d, DeliverFromTime):
        return {"from_time": d.time_ms}
    raise ExspeedError(f"invalid deliver policy: {d!r}")


def _deliver_from_json(v: Any) -> DeliverPolicy:
    if v == "new":
        return "new"
    if isinstance(v, Mapping):
        if "from_offset" in v:
            return DeliverFromOffset(int(v["from_offset"]))
        if "from_time" in v:
            return DeliverFromTime(int(v["from_time"]))
    return "all"


@dataclass
class ConsumerSpec:
    """A consumer's settings. Only ``name`` and ``stream`` are required;
    ``None`` fields take the server's defaults (shown in brackets)."""

    name: str
    stream: str
    #: Subject filters (``orders.placed``, ``orders.eu.>``). [all subjects]
    filter_subjects: list[str] | None = None
    #: Where a new consumer starts. [``"all"``]
    deliver: DeliverPolicy | None = None
    #: ``"explicit"``: each record must be acked. ``"none"``: at-most-once. [``"explicit"``]
    ack: Literal["explicit", "none"] | None = None
    #: Redeliver a record not acked within this time. [30000]
    ack_wait_ms: int | None = None
    #: Dead-letter after this many deliveries; 0 = never. [5]
    max_deliver: int | None = None
    #: Redelivery delays by delivery count (the last repeats). [redeliver immediately]
    backoff_ms: list[int] | None = None
    #: Pause delivery while this many records await an ack. [1000]
    max_ack_pending: int | None = None
    #: Stream that receives dead letters. [dropped and counted]
    dlq_stream: str | None = None
    #: Deleted when the connection that created it closes. [False]
    ephemeral: bool | None = None
    #: Dead-letter records whose TTL ends before they are acked (reason ``expired``). [False]
    dead_letter_expired: bool | None = None
    #: Only records whose headers have these exact values, combined by ``header_match``.
    filter_headers: dict[str, str] | None = None
    #: ``"all"``: every ``filter_headers`` entry must match; ``"any"``: at least one. [``"all"``]
    header_match: Literal["all", "any"] | None = None
    #: Deliver to one subscription at a time (the oldest); pulls are refused. [False]
    single_active: bool | None = None
    #: Look this many records ahead and deliver higher ``priority`` first (max 10000). [0]
    priority_window: int | None = None

    def to_wire(self) -> dict[str, Any]:
        """The snake_case JSON object, keys in the Rust ``ConsumerSpec`` order, unset fields left out."""
        if not self.name:
            raise ExspeedError("consumer name is required")
        if not self.stream:
            raise ExspeedError("consumer stream is required")
        out: dict[str, Any] = {"name": self.name, "stream": self.stream}

        def put(key: str, v: Any) -> None:
            if v is not None:
                out[key] = v

        put("filter_subjects", None if self.filter_subjects is None else list(self.filter_subjects))
        put("deliver", None if self.deliver is None else _deliver_to_wire(self.deliver))
        put("ack", self.ack)
        put("ack_wait_ms", self.ack_wait_ms)
        put("max_deliver", self.max_deliver)
        put("backoff_ms", None if self.backoff_ms is None else list(self.backoff_ms))
        put("max_ack_pending", self.max_ack_pending)
        put("dlq_stream", self.dlq_stream)
        put("ephemeral", self.ephemeral)
        put("dead_letter_expired", self.dead_letter_expired)
        if self.filter_headers:
            out["filter_headers"] = {str(k): str(v) for k, v in self.filter_headers.items()}
        put("header_match", self.header_match)
        put("single_active", self.single_active)
        put("priority_window", self.priority_window)
        return out

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> ConsumerSpec:
        """Build from the server's JSON (every field filled in)."""
        fh = d.get("filter_headers")
        return cls(
            name=str(d.get("name", "")),
            stream=str(d.get("stream", "")),
            filter_subjects=[str(s) for s in d.get("filter_subjects") or []],
            deliver=_deliver_from_json(d.get("deliver", "all")),
            ack="none" if d.get("ack") == "none" else "explicit",
            ack_wait_ms=_int(d, "ack_wait_ms", 30_000),
            max_deliver=_int(d, "max_deliver", 5),
            backoff_ms=[int(x) for x in d.get("backoff_ms") or []],
            max_ack_pending=_int(d, "max_ack_pending", 1_000),
            dlq_stream=d.get("dlq_stream") if isinstance(d.get("dlq_stream"), str) else None,
            ephemeral=bool(d.get("ephemeral", False)),
            dead_letter_expired=bool(d.get("dead_letter_expired", False)),
            filter_headers={str(k): str(v) for k, v in fh.items()} if isinstance(fh, Mapping) else {},
            header_match="any" if d.get("header_match") == "any" else "all",
            single_active=bool(d.get("single_active", False)),
            priority_window=_int(d, "priority_window"),
        )


@dataclass
class ConsumerStats:
    """Lifetime counters of a consumer."""

    delivered: int = 0
    redelivered: int = 0
    acked: int = 0
    dead_lettered: int = 0
    gone: int = 0
    skipped: int = 0

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> ConsumerStats:
        """Build from the server's JSON."""
        return cls(
            delivered=_int(d, "delivered"),
            redelivered=_int(d, "redelivered"),
            acked=_int(d, "acked"),
            dead_lettered=_int(d, "dead_lettered"),
            gone=_int(d, "gone"),
            skipped=_int(d, "skipped"),
        )


@dataclass
class ConsumerInfo:
    """A consumer's settings, position and counters."""

    spec: ConsumerSpec
    #: Next stream offset to be delivered for the first time.
    next_offset: int
    #: Everything below this offset is acked (or filtered out).
    ack_floor: int
    num_unacked: int
    num_in_flight: int
    #: Records held back until their delivery time (``delay`` / ``deliver_at``).
    num_delayed: int
    #: Records not yet delivered (approximate).
    num_waiting: int
    lag: int
    subscribers: int
    pull_waiters: int
    stats: ConsumerStats
    #: The JSON object as received.
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> ConsumerInfo:
        """Build from the server's JSON."""
        spec = d.get("spec")
        stats = d.get("stats")
        return cls(
            spec=ConsumerSpec.from_json(spec if isinstance(spec, Mapping) else {}),
            next_offset=_int(d, "next_offset"),
            ack_floor=_int(d, "ack_floor"),
            num_unacked=_int(d, "num_unacked"),
            num_in_flight=_int(d, "num_in_flight"),
            num_delayed=_int(d, "num_delayed"),
            num_waiting=_int(d, "num_waiting"),
            lag=_int(d, "lag"),
            subscribers=_int(d, "subscribers"),
            pull_waiters=_int(d, "pull_waiters"),
            stats=ConsumerStats.from_json(stats if isinstance(stats, Mapping) else {}),
            raw=dict(d),
        )


@dataclass(frozen=True)
class SeekTime:
    """Seek target: the first record at or after this time, in ms since the epoch."""

    time_ms: int


#: Where to move a consumer's cursor: ``"earliest"``, ``"latest"``, an offset
#: (``int``), a :class:`datetime.datetime`, or a :class:`SeekTime`.
SeekTarget = Literal["earliest", "latest"] | int | datetime | SeekTime


# ---------------------------------------------------------------------------
# Connection options and server info
# ---------------------------------------------------------------------------


@dataclass
class ReconnectOptions:
    """How the client reconnects after the connection drops."""

    #: Give up after this many failed attempts in a row; ``None`` = never.
    max_attempts: int | None = None
    #: Delay before the first attempt (seconds); doubles each attempt.
    initial_delay: float = 0.1
    #: Upper bound for the delay between attempts (seconds).
    max_delay: float = 5.0


@dataclass
class TlsOptions:
    """TLS settings. Certificates are verified against ``ca_file`` / ``ca_data``,
    or the system CAs when neither is given."""

    #: PEM file of the CA(s) to trust.
    ca_file: str | None = None
    #: PEM text of the CA(s) to trust.
    ca_data: str | bytes | None = None
    #: Client certificate chain (PEM file) for mutual TLS.
    cert_file: str | None = None
    #: Private key of ``cert_file`` (PEM file); defaults to ``cert_file``.
    key_file: str | None = None
    #: Password of an encrypted ``key_file``.
    key_password: str | None = None
    #: Name to verify the server certificate against and send as SNI; defaults to the host.
    server_hostname: str | None = None
    #: ``False`` disables certificate verification (testing only).
    verify: bool = True

    def context(self) -> ssl.SSLContext:
        """Build the :class:`ssl.SSLContext`."""
        cadata = self.ca_data.decode("ascii") if isinstance(self.ca_data, bytes) else self.ca_data
        if self.ca_file or cadata:
            ctx = ssl.create_default_context(cafile=self.ca_file, cadata=cadata)
        else:
            ctx = ssl.create_default_context()
        if not self.verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if self.cert_file:
            ctx.load_cert_chain(self.cert_file, self.key_file, self.key_password)
        return ctx


@dataclass(frozen=True)
class ServerInfo:
    """From the server's handshake reply."""

    server_version: str
    node_id: str
    #: The leader's client address when the connected node is not the leader.
    leader: str | None


@dataclass(frozen=True)
class Metadata:
    """Node id, leadership and version of the connected server."""

    node_id: str
    is_leader: bool
    leader: str | None
    server_version: str


@dataclass
class QueryResult:
    """Result of a bounded ExQL query."""

    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    execution_time_ms: int
    #: True when the server's row cap cut the result short.
    truncated: bool = False


def parse_json(data: bytes, what: str) -> Any:
    """Decode a JSON reply."""
    try:
        return json.loads(data)
    except ValueError as e:
        raise ProtocolError(f"bad JSON in reply to {what}: {e}") from None
