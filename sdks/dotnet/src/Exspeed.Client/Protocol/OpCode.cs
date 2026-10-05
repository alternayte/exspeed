namespace Exspeed;

/// <summary>Constants of client protocol v2.</summary>
public static class ProtocolConstants
{
    /// <summary>Wire protocol version spoken by this library (byte 0 of every frame).</summary>
    public const byte Version = 0x02;

    /// <summary>Size of a frame header: <c>[version u8][opcode u8][correlation id u32][payload length u32]</c>.</summary>
    public const int FrameHeaderSize = 10;

    /// <summary>Largest payload either side accepts (16 MiB).</summary>
    public const int MaxPayloadSize = 16 * 1024 * 1024;

    /// <summary>The default client port.</summary>
    public const int DefaultPort = 5933;
}

/// <summary>Operation codes of client protocol v2 (see <c>docs/protocol.md</c>).</summary>
internal enum OpCode : byte
{
    // Requests (client -> server)
    Connect = 0x01,
    Metadata = 0x03,
    Publish = 0x10,
    PublishBatch = 0x11,
    CreateStream = 0x18,
    UpdateStream = 0x19,
    DeleteStream = 0x1a,
    StreamInfo = 0x1b,
    ListStreams = 0x1c,
    Query = 0x20,
    CreateConsumer = 0x40,
    DeleteConsumer = 0x41,
    ConsumerInfo = 0x42,
    ListConsumers = 0x43,
    SeekConsumer = 0x44,
    Subscribe = 0x50,
    Credit = 0x51,
    Unsubscribe = 0x52,
    Pull = 0x53,
    Ack = 0x54,
    Nack = 0x55,
    Term = 0x56,
    InProgress = 0x57,
    Read = 0x60,
    CorePublish = 0x70,
    CoreSubscribe = 0x71,
    KvPut = 0x74,
    KvGet = 0x75,
    KvDelete = 0x76,
    KvKeys = 0x77,
    KvHistory = 0x78,
    KvCreateBucket = 0x79,
    Ping = 0xf0,

    // Responses and pushes (server -> client)
    Ok = 0x80,
    Error = 0x81,
    Deliver = 0x82,
    Messages = 0x83,
    ReadResult = 0x84,
    Json = 0x85,
    PublishOk = 0x86,
    PublishBatchOk = 0x87,
    ConnectOk = 0x88,
    SubscribeOk = 0x89,
    SubscriptionEnded = 0x8a,
    CoreMsg = 0x8b,
    Pong = 0xf1,
}
