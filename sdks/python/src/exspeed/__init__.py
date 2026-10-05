"""Python client for Exspeed, speaking the binary client protocol v2 over TCP or TLS.

Quick start::

    import asyncio
    import exspeed

    async def main() -> None:
        async with exspeed.connect("127.0.0.1", 5933) as client:
            await client.create_stream("orders")
            await client.publish("orders", "orders.placed", {"id": 42})

    asyncio.run(main())
"""

from __future__ import annotations

from .client import Event, ExspeedClient, ReadResult, connect
from .core import CoreMessage, CoreSubscription
from .errors import ConnectionError, ExspeedError, ProtocolError, ServerError, TimeoutError
from .kv import KV_OP_HEADER, KvBucket, KvEntry, KvOp, KvWatch
from .message import Message, StreamRecord
from .msg_id import new_msg_id
from .protocol.constants import DEFAULT_PORT, PROTOCOL_VERSION, ErrorCode, OpCode
from .publisher import Publisher
from .subscription import EndReason, Subscription
from .types import (
    DELAY_HEADER,
    DELIVER_AT_HEADER,
    MSG_ID_HEADER,
    PRIORITY_HEADER,
    TTL_HEADER,
    ConsumerInfo,
    ConsumerSpec,
    ConsumerStats,
    DeliverFromOffset,
    DeliverFromTime,
    DeliverPolicy,
    Duration,
    HeadersInit,
    Metadata,
    PublishRecord,
    PublishResult,
    QueryResult,
    ReconnectOptions,
    SeekTarget,
    SeekTime,
    ServerInfo,
    StreamConfig,
    StreamInfo,
    StreamSpec,
    TlsOptions,
    Value,
)

__version__ = "0.7.0"

__all__ = [
    "__version__",
    # client
    "connect",
    "ExspeedClient",
    "Event",
    "ReadResult",
    "Publisher",
    "Subscription",
    "EndReason",
    "Message",
    "StreamRecord",
    "CoreMessage",
    "CoreSubscription",
    "KvBucket",
    "KvEntry",
    "KvWatch",
    "KvOp",
    "KV_OP_HEADER",
    "new_msg_id",
    # types
    "StreamSpec",
    "StreamConfig",
    "StreamInfo",
    "PublishRecord",
    "PublishResult",
    "ConsumerSpec",
    "ConsumerInfo",
    "ConsumerStats",
    "DeliverPolicy",
    "DeliverFromOffset",
    "DeliverFromTime",
    "SeekTarget",
    "SeekTime",
    "ReconnectOptions",
    "TlsOptions",
    "ServerInfo",
    "Metadata",
    "QueryResult",
    "Value",
    "HeadersInit",
    "Duration",
    "TTL_HEADER",
    "DELAY_HEADER",
    "DELIVER_AT_HEADER",
    "PRIORITY_HEADER",
    "MSG_ID_HEADER",
    # errors
    "ExspeedError",
    "ServerError",
    "ConnectionError",
    "TimeoutError",
    "ProtocolError",
    # protocol
    "ErrorCode",
    "OpCode",
    "PROTOCOL_VERSION",
    "DEFAULT_PORT",
]
