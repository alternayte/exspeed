"""Binary building blocks of client protocol v2: integers, strings, options,
headers, records (with their CRC32C) and frames.

All integers are little-endian:

- ``str``     = ``u16`` length + UTF-8 bytes
- ``lstr``    = ``u32`` length + UTF-8 bytes (SQL text)
- ``bytes``   = ``u32`` length + raw bytes
- ``opt<T>``  = ``u8`` flag (0 absent, 1 present) + ``T``
- ``headers`` = ``u16`` count + (``str`` key, ``str`` value) pairs
- ``vec<T>``  = ``u32`` count + items
"""

from __future__ import annotations

import struct
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import NamedTuple, TypeVar

from ..errors import ExspeedError, ProtocolError
from .constants import (
    FRAME_HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
    MAX_RECORD_LEN,
    MIN_RECORD_LEN,
    PROTOCOL_VERSION,
)

__all__ = [
    "Headers",
    "Writer",
    "Reader",
    "WireRecord",
    "Frame",
    "FrameParser",
    "crc32c",
    "encode_frame",
    "encode_record",
    "verify_record_crc",
]

#: Record or message headers, in wire order (a key can repeat).
Headers = list[tuple[str, str]]

T = TypeVar("T")

_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_FRAME_HEADER = struct.Struct("<BBII")
_RECORD_HEADER = struct.Struct("<IIH")

_U16_MAX = 0xFFFF
_U32_MAX = 0xFFFFFFFF
_U64_MAX = 0xFFFFFFFFFFFFFFFF


def _make_crc_table() -> list[int]:
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        table.append(c)
    return table


_CRC_TABLE = _make_crc_table()


def crc32c(data: bytes | bytearray | memoryview) -> int:
    """CRC32C (Castagnoli) of ``data``, as an unsigned 32-bit int.

    Every :class:`WireRecord` carries the CRC32C of its bytes after
    ``delivery_count``. This is a pure-Python implementation, so decoding
    verifies it only when asked (``verify_crc=True`` on the client).
    """
    crc = 0xFFFFFFFF
    table = _CRC_TABLE
    for b in bytes(data):
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


class Writer:
    """Little-endian payload writer.

    A value that doesn't fit its wire type (a string over 65,535 bytes, a
    negative integer, ...) raises :class:`~exspeed.ExspeedError` instead of
    being truncated.
    """

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def finish(self) -> bytes:
        """The encoded bytes."""
        return bytes(self._buf)

    def __len__(self) -> int:
        return len(self._buf)

    @staticmethod
    def _check_int(v: int, bits: int) -> None:
        if not isinstance(v, int) or isinstance(v, bool) or v < 0 or v >> bits:
            raise ExspeedError(f"{v!r} does not fit an unsigned {bits}-bit integer")

    def u8(self, v: int) -> Writer:
        """Append an unsigned byte."""
        self._check_int(v, 8)
        self._buf.append(v)
        return self

    def u16(self, v: int) -> Writer:
        """Append a ``u16``."""
        self._check_int(v, 16)
        self._buf += _U16.pack(v)
        return self

    def u32(self, v: int) -> Writer:
        """Append a ``u32``."""
        self._check_int(v, 32)
        self._buf += _U32.pack(v)
        return self

    def u64(self, v: int) -> Writer:
        """Append a ``u64``."""
        self._check_int(v, 64)
        self._buf += _U64.pack(v)
        return self

    def raw(self, b: bytes | bytearray | memoryview) -> Writer:
        """Append bytes without a length prefix."""
        self._buf += b
        return self

    def string(self, s: str) -> Writer:
        """Append a ``str`` (``u16`` length + UTF-8)."""
        b = s.encode("utf-8")
        if len(b) > _U16_MAX:
            raise ExspeedError(f"string of {len(b)} bytes exceeds the {_U16_MAX} byte limit")
        self._buf += _U16.pack(len(b))
        self._buf += b
        return self

    def long_string(self, s: str) -> Writer:
        """Append an ``lstr`` (``u32`` length + UTF-8)."""
        return self.byte_string(s.encode("utf-8"))

    def byte_string(self, b: bytes | bytearray | memoryview) -> Writer:
        """Append ``bytes`` (``u32`` length + raw bytes)."""
        n = len(b)
        if n > _U32_MAX:
            raise ExspeedError(f"byte string of {n} bytes is too long")
        self._buf += _U32.pack(n)
        self._buf += b
        return self

    def optional(self, v: T | None, f: Callable[[Writer, T], object]) -> Writer:
        """Append ``opt<T>``: a flag byte, then ``f(writer, v)`` when ``v`` is not ``None``."""
        if v is None:
            self._buf.append(0)
        else:
            self._buf.append(1)
            f(self, v)
        return self

    def headers(self, h: Sequence[tuple[str, str]]) -> Writer:
        """Append ``headers``."""
        if len(h) > _U16_MAX:
            raise ExspeedError(f"{len(h)} headers exceed the limit of {_U16_MAX}")
        self._buf += _U16.pack(len(h))
        for k, v in h:
            self.string(k)
            self.string(v)
        return self

    def record(self, r: WireRecord) -> Writer:
        """Append one encoded :class:`WireRecord` (length, CRC, delivery count, fields)."""
        self._buf += encode_record(r)
        return self

    def records(self, records: Sequence[WireRecord]) -> Writer:
        """Append ``vec<WireRecord>``."""
        self.u32(len(records))
        for r in records:
            self.record(r)
        return self


class Reader:
    """Bounds-checked payload reader.

    Truncated input, trailing bytes, invalid UTF-8, bad option flags and
    counts larger than the remaining payload could hold raise
    :class:`~exspeed.ProtocolError`.
    """

    __slots__ = ("_buf", "_pos", "verify_crc")

    def __init__(self, buf: bytes | bytearray | memoryview, *, verify_crc: bool = False) -> None:
        self._buf = memoryview(buf)
        self._pos = 0
        #: Check each record's CRC32C while decoding.
        self.verify_crc = verify_crc

    @property
    def remaining(self) -> int:
        """Bytes not yet read."""
        return len(self._buf) - self._pos

    def _need(self, n: int) -> None:
        if len(self._buf) - self._pos < n:
            raise ProtocolError(f"truncated payload: need {n} bytes, have {len(self._buf) - self._pos}")

    def finish(self) -> None:
        """Raise if bytes are left over (catches encoder/decoder drift)."""
        if self._pos != len(self._buf):
            raise ProtocolError(f"{len(self._buf) - self._pos} trailing bytes")

    def u8(self) -> int:
        """Read an unsigned byte."""
        self._need(1)
        v = self._buf[self._pos]
        self._pos += 1
        return v

    def u16(self) -> int:
        """Read a ``u16``."""
        self._need(2)
        (v,) = _U16.unpack_from(self._buf, self._pos)
        self._pos += 2
        return int(v)

    def u32(self) -> int:
        """Read a ``u32``."""
        self._need(4)
        (v,) = _U32.unpack_from(self._buf, self._pos)
        self._pos += 4
        return int(v)

    def u64(self) -> int:
        """Read a ``u64``."""
        self._need(8)
        (v,) = _U64.unpack_from(self._buf, self._pos)
        self._pos += 8
        return int(v)

    def take(self, n: int) -> memoryview:
        """The next ``n`` bytes (a view, not a copy)."""
        self._need(n)
        v = self._buf[self._pos : self._pos + n]
        self._pos += n
        return v

    @staticmethod
    def _utf8(b: memoryview) -> str:
        try:
            return bytes(b).decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("invalid UTF-8") from None

    def string(self) -> str:
        """Read a ``str``."""
        return self._utf8(self.take(self.u16()))

    def long_string(self) -> str:
        """Read an ``lstr``."""
        return self._utf8(self.take(self.u32()))

    def byte_string(self) -> bytes:
        """Read ``bytes``."""
        return bytes(self.take(self.u32()))

    def optional(self, f: Callable[[Reader], T]) -> T | None:
        """Read ``opt<T>`` with ``f`` reading the value."""
        flag = self.u8()
        if flag == 0:
            return None
        if flag == 1:
            return f(self)
        raise ProtocolError(f"invalid option flag {flag}")

    def count(self, min_size: int) -> int:
        """Read a ``u32`` element count, rejecting counts the rest of the payload can't hold."""
        n = self.u32()
        if n * max(min_size, 1) > self.remaining:
            raise ProtocolError(f"count {n} exceeds payload size")
        return n

    def headers(self) -> Headers:
        """Read ``headers``."""
        n = self.u16()
        if n * 4 > self.remaining:
            raise ProtocolError("header count exceeds payload")
        return [(self.string(), self.string()) for _ in range(n)]

    def record(self) -> WireRecord:
        """Read one :class:`WireRecord`, checking its length field (and CRC when ``verify_crc``)."""
        self._need(4)
        (length,) = _U32.unpack_from(self._buf, self._pos)
        size = int(length) + 4
        if not MIN_RECORD_LEN <= size <= MAX_RECORD_LEN:
            raise ProtocolError(f"record: record length {size} out of bounds")
        rec = self.take(size)
        _, crc, delivery_count = _RECORD_HEADER.unpack_from(rec, 0)
        if self.verify_crc and crc32c(rec[10:]) != crc:
            raise ProtocolError(f"record: CRC mismatch: stored {crc:#010x}, computed {crc32c(rec[10:]):#010x}")
        r = Reader(rec[10:])
        try:
            out = WireRecord(
                offset=r.u64(),
                timestamp_ns=r.u64(),
                delivery_count=int(delivery_count),
                subject=r.string(),
                key=r.optional(Reader.byte_string),
                value=r.byte_string(),
                headers=r.headers(),
            )
            r.finish()
        except ProtocolError as e:
            raise ProtocolError(f"record: length field says {size} bytes, but its fields disagree ({e})") from None
        return out

    def records(self) -> list[WireRecord]:
        """Read ``vec<WireRecord>``."""
        n = self.count(MIN_RECORD_LEN)
        return [self.record() for _ in range(n)]


@dataclass
class WireRecord:
    """A record as the server stores and sends it (``WireRecord`` in ``docs/protocol.md``)."""

    offset: int
    #: Append time, nanoseconds since the Unix epoch.
    timestamp_ns: int
    #: 1 on first delivery to a consumer, +1 per redelivery; 0 for stateless reads.
    delivery_count: int
    subject: str
    key: bytes | None
    value: bytes
    headers: Headers = field(default_factory=list)


def encode_record(r: WireRecord) -> bytes:
    """Encode one record: ``u32 len``, ``u32 crc``, ``u16 delivery_count``, then the fields."""
    w = Writer()
    w.u64(r.offset).u64(r.timestamp_ns).string(r.subject)
    w.optional(r.key, Writer.byte_string).byte_string(r.value).headers(r.headers)
    body = w.finish()
    size = 10 + len(body)
    if size > MAX_RECORD_LEN:
        raise ExspeedError(f"encoded record is {size} bytes; the limit is {MAX_RECORD_LEN}")
    Writer._check_int(r.delivery_count, 16)
    return _RECORD_HEADER.pack(size - 4, crc32c(body), r.delivery_count) + body


def verify_record_crc(record: bytes | bytearray | memoryview) -> bool:
    """True when one encoded record's stored CRC matches its contents."""
    if len(record) < MIN_RECORD_LEN:
        return False
    (stored,) = _U32.unpack_from(record, 4)
    return bool(stored == crc32c(memoryview(record)[10:]))


class Frame(NamedTuple):
    """One protocol frame: header fields plus the raw payload."""

    opcode: int
    correlation_id: int
    payload: bytes


def encode_frame(opcode: int, correlation_id: int, payload: bytes | bytearray) -> bytes:
    """Serialize a frame: ``[version][opcode][corr u32 LE][len u32 LE][payload]``."""
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ExspeedError(f"payload too large: {len(payload)} bytes (max {MAX_PAYLOAD_SIZE}); split the batch")
    return _FRAME_HEADER.pack(PROTOCOL_VERSION, opcode, correlation_id, len(payload)) + bytes(payload)


class FrameParser:
    """Incremental frame parser for a byte stream.

    Feed it socket chunks with :meth:`push`; it returns every complete frame.
    A bad version or an oversize length raises :class:`~exspeed.ProtocolError`:
    the stream can't be resynchronised after that, so drop the connection.
    """

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def push(self, chunk: bytes | bytearray | memoryview) -> list[Frame]:
        """Add bytes and return the frames they complete."""
        self._buf += chunk
        buf = self._buf
        frames: list[Frame] = []
        pos = 0
        n = len(buf)
        while n - pos >= FRAME_HEADER_SIZE:
            version, opcode, corr, length = _FRAME_HEADER.unpack_from(buf, pos)
            if version != PROTOCOL_VERSION:
                raise ProtocolError(f"unsupported protocol version {version:#x}")
            if length > MAX_PAYLOAD_SIZE:
                raise ProtocolError(f"payload too large: {length} bytes (max {MAX_PAYLOAD_SIZE})")
            end = pos + FRAME_HEADER_SIZE + length
            if n < end:
                break
            frames.append(Frame(opcode, corr, bytes(buf[pos + FRAME_HEADER_SIZE : end])))
            pos = end
        if pos:
            del buf[:pos]
        return frames

    @property
    def pending(self) -> int:
        """Bytes buffered towards an incomplete frame."""
        return len(self._buf)
