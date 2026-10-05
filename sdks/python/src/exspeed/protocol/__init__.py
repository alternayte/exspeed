"""Client protocol v2: constants, binary codec and message types.

Applications don't need this package; it is public for tools, tests and
anyone implementing a compatible peer. The message dataclasses live in
:mod:`exspeed.protocol.messages`.
"""

from __future__ import annotations

from . import messages
from .codec import (
    Frame,
    FrameParser,
    Headers,
    Reader,
    WireRecord,
    Writer,
    crc32c,
    encode_frame,
    encode_record,
    verify_record_crc,
)
from .constants import (
    DEFAULT_PORT,
    FRAME_HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
    MAX_RECORD_LEN,
    MIN_RECORD_LEN,
    PROTOCOL_VERSION,
    ErrorCode,
    OpCode,
    SeekKind,
)
from .messages import (
    Request,
    Response,
    WirePublishRecord,
    WireStreamSpec,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
    request_frame,
    response_frame,
)

__all__ = [
    "messages",
    "Frame",
    "FrameParser",
    "Headers",
    "Reader",
    "WireRecord",
    "Writer",
    "crc32c",
    "encode_frame",
    "encode_record",
    "verify_record_crc",
    "DEFAULT_PORT",
    "FRAME_HEADER_SIZE",
    "MAX_PAYLOAD_SIZE",
    "MAX_RECORD_LEN",
    "MIN_RECORD_LEN",
    "PROTOCOL_VERSION",
    "ErrorCode",
    "OpCode",
    "SeekKind",
    "Request",
    "Response",
    "WirePublishRecord",
    "WireStreamSpec",
    "decode_request",
    "decode_response",
    "encode_request",
    "encode_response",
    "request_frame",
    "response_frame",
]
