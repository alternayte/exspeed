namespace Exspeed.Protocol;

/// <summary>
/// Every request of client protocol v2 with its binary encoding, mirroring <c>Request</c> in
/// <c>crates/exspeed-protocol/src/client.rs</c>. Decoding (what the server does) is used by the unit
/// tests' fake server.
/// </summary>
internal abstract record Request
{
    public abstract OpCode Op { get; }

    /// <summary>A short name for error messages ("Publish", "Read", ...).</summary>
    public string Name => GetType().Name;

    public abstract void Encode(WireWriter w);

    /// <summary>The payload alone (no frame header).</summary>
    public byte[] EncodePayload()
    {
        var w = new WireWriter();
        Encode(w);
        return w.ToArray();
    }

    /// <summary>The complete frame.</summary>
    public byte[] ToFrame(uint correlationId)
    {
        var w = WireWriter.ForFrame();
        Encode(w);
        return w.FinishFrame(Op, correlationId);
    }

    public sealed record Connect(string ClientId, string? Token) : Request
    {
        public override OpCode Op => OpCode.Connect;
        public override void Encode(WireWriter w) => w.Str(ClientId).OptStr(Token);
    }

    public sealed record Ping : Request
    {
        public override OpCode Op => OpCode.Ping;
        public override void Encode(WireWriter w) { }
    }

    public sealed record Metadata : Request
    {
        public override OpCode Op => OpCode.Metadata;
        public override void Encode(WireWriter w) { }
    }

    public sealed record Publish(string Stream, WirePublishRecord Record) : Request
    {
        public override OpCode Op => OpCode.Publish;
        public override void Encode(WireWriter w)
        {
            w.Str(Stream);
            Record.Encode(w);
        }
    }

    public sealed record PublishBatch(string Stream, IReadOnlyList<WirePublishRecord> Records) : Request
    {
        public override OpCode Op => OpCode.PublishBatch;
        public override void Encode(WireWriter w)
        {
            w.Str(Stream);
            w.U32((uint)Records.Count);
            foreach (var r in Records)
            {
                r.Encode(w);
            }
        }
    }

    public sealed record CreateStream(WireStreamSpec Spec) : Request
    {
        public override OpCode Op => OpCode.CreateStream;
        public override void Encode(WireWriter w) => Spec.Encode(w);
    }

    public sealed record UpdateStream(WireStreamSpec Spec) : Request
    {
        public override OpCode Op => OpCode.UpdateStream;
        public override void Encode(WireWriter w) => Spec.Encode(w);
    }

    public sealed record DeleteStream(string StreamName) : Request
    {
        public override OpCode Op => OpCode.DeleteStream;
        public override void Encode(WireWriter w) => w.Str(StreamName);
    }

    public sealed record StreamInfo(string StreamName) : Request
    {
        public override OpCode Op => OpCode.StreamInfo;
        public override void Encode(WireWriter w) => w.Str(StreamName);
    }

    public sealed record ListStreams : Request
    {
        public override OpCode Op => OpCode.ListStreams;
        public override void Encode(WireWriter w) { }
    }

    public sealed record Query(string Sql) : Request
    {
        public override OpCode Op => OpCode.Query;
        public override void Encode(WireWriter w) => w.LStr(Sql);
    }

    /// <summary><paramref name="SpecJson"/> is the snake_case ConsumerSpec JSON.</summary>
    public sealed record CreateConsumer(ReadOnlyMemory<byte> SpecJson) : Request
    {
        public override OpCode Op => OpCode.CreateConsumer;
        public override void Encode(WireWriter w) => w.Bytes(SpecJson.Span);
    }

    public sealed record DeleteConsumer(string Consumer) : Request
    {
        public override OpCode Op => OpCode.DeleteConsumer;
        public override void Encode(WireWriter w) => w.Str(Consumer);
    }

    public sealed record ConsumerInfo(string Consumer) : Request
    {
        public override OpCode Op => OpCode.ConsumerInfo;
        public override void Encode(WireWriter w) => w.Str(Consumer);
    }

    public sealed record ListConsumers(string? Stream) : Request
    {
        public override OpCode Op => OpCode.ListConsumers;
        public override void Encode(WireWriter w) => w.OptStr(Stream);
    }

    /// <summary>Kind: 0 earliest, 1 latest, 2 offset, 3 time (ms since the epoch).</summary>
    public sealed record SeekConsumer(string Consumer, byte Kind, ulong Value) : Request
    {
        public override OpCode Op => OpCode.SeekConsumer;
        public override void Encode(WireWriter w) => w.Str(Consumer).U8(Kind).U64(Value);
    }

    public sealed record Subscribe(string Consumer, uint Credits) : Request
    {
        public override OpCode Op => OpCode.Subscribe;
        public override void Encode(WireWriter w) => w.Str(Consumer).U32(Credits);
    }

    public sealed record Credit(uint SubId, uint Credits) : Request
    {
        public override OpCode Op => OpCode.Credit;
        public override void Encode(WireWriter w) => w.U32(SubId).U32(Credits);
    }

    public sealed record Unsubscribe(uint SubId) : Request
    {
        public override OpCode Op => OpCode.Unsubscribe;
        public override void Encode(WireWriter w) => w.U32(SubId);
    }

    public sealed record Pull(string Consumer, uint MaxMessages, uint MaxBytes, uint ExpiresMs) : Request
    {
        public override OpCode Op => OpCode.Pull;
        public override void Encode(WireWriter w) => w.Str(Consumer).U32(MaxMessages).U32(MaxBytes).U32(ExpiresMs);
    }

    public sealed record Ack(string Consumer, IReadOnlyList<ulong> Offsets) : Request
    {
        public override OpCode Op => OpCode.Ack;
        public override void Encode(WireWriter w) => EncodeOffsets(w, Consumer, Offsets);
    }

    public sealed record Nack(string Consumer, ulong Offset, uint DelayMs) : Request
    {
        public override OpCode Op => OpCode.Nack;
        public override void Encode(WireWriter w) => w.Str(Consumer).U64(Offset).U32(DelayMs);
    }

    public sealed record Term(string Consumer, ulong Offset, string Reason) : Request
    {
        public override OpCode Op => OpCode.Term;
        public override void Encode(WireWriter w) => w.Str(Consumer).U64(Offset).Str(Reason);
    }

    public sealed record InProgress(string Consumer, IReadOnlyList<ulong> Offsets) : Request
    {
        public override OpCode Op => OpCode.InProgress;
        public override void Encode(WireWriter w) => EncodeOffsets(w, Consumer, Offsets);
    }

    public sealed record Read(string Stream, ulong From, uint MaxRecords, uint MaxBytes, uint WaitMs, string Filter) : Request
    {
        public override OpCode Op => OpCode.Read;
        public override void Encode(WireWriter w) =>
            w.Str(Stream).U64(From).U32(MaxRecords).U32(MaxBytes).U32(WaitMs).Str(Filter);
    }

    /// <summary>Correlation id 0 = fire-and-forget; with a reply subject, 404 when nobody received it.</summary>
    public sealed record CorePublish(
        string Subject,
        string? ReplyTo,
        IReadOnlyList<KeyValuePair<string, string>> Headers,
        ReadOnlyMemory<byte> Value) : Request
    {
        public override OpCode Op => OpCode.CorePublish;
        public override void Encode(WireWriter w) => w.Str(Subject).OptStr(ReplyTo).Headers(Headers).Bytes(Value.Span);
    }

    public sealed record CoreSubscribe(string Subject, string? Queue) : Request
    {
        public override OpCode Op => OpCode.CoreSubscribe;
        public override void Encode(WireWriter w) => w.Str(Subject).OptStr(Queue);
    }

    public sealed record KvCreateBucket(string Bucket, ulong History, ulong TtlMs, ulong MaxBytes) : Request
    {
        public override OpCode Op => OpCode.KvCreateBucket;
        public override void Encode(WireWriter w) => w.Str(Bucket).U64(History).U64(TtlMs).U64(MaxBytes);
    }

    public sealed record KvPut(string Bucket, string Key, ReadOnlyMemory<byte> Value, ulong? ExpectedRevision, ulong? TtlMs) : Request
    {
        public override OpCode Op => OpCode.KvPut;
        public override void Encode(WireWriter w) =>
            w.Str(Bucket).Str(Key).Bytes(Value.Span).OptU64(ExpectedRevision).OptU64(TtlMs);
    }

    public sealed record KvGet(string Bucket, string Key, ulong? Revision) : Request
    {
        public override OpCode Op => OpCode.KvGet;
        public override void Encode(WireWriter w) => w.Str(Bucket).Str(Key).OptU64(Revision);
    }

    public sealed record KvDelete(string Bucket, string Key, bool Purge, ulong? ExpectedRevision) : Request
    {
        public override OpCode Op => OpCode.KvDelete;
        public override void Encode(WireWriter w) => w.Str(Bucket).Str(Key).Bool(Purge).OptU64(ExpectedRevision);
    }

    public sealed record KvKeys(string Bucket, string Filter) : Request
    {
        public override OpCode Op => OpCode.KvKeys;
        public override void Encode(WireWriter w) => w.Str(Bucket).Str(Filter);
    }

    public sealed record KvHistory(string Bucket, string Key) : Request
    {
        public override OpCode Op => OpCode.KvHistory;
        public override void Encode(WireWriter w) => w.Str(Bucket).Str(Key);
    }

    private static void EncodeOffsets(WireWriter w, string consumer, IReadOnlyList<ulong> offsets)
    {
        w.Str(consumer);
        w.U32((uint)offsets.Count);
        foreach (var o in offsets)
        {
            w.U64(o);
        }
    }

    private static List<ulong> DecodeOffsets(WireReader r)
    {
        int n = r.Count(8);
        var list = new List<ulong>(n);
        for (int i = 0; i < n; i++)
        {
            list.Add(r.U64());
        }
        return list;
    }

    /// <summary>Decode a request payload (what the server does).</summary>
    public static Request Decode(byte opcode, ReadOnlyMemory<byte> payload)
    {
        var r = new WireReader(payload);
        Request req = (OpCode)opcode switch
        {
            OpCode.Connect => new Connect(r.Str(), r.OptStr()),
            OpCode.Ping => new Ping(),
            OpCode.Metadata => new Metadata(),
            OpCode.Publish => new Publish(r.Str(), WirePublishRecord.Decode(r)),
            OpCode.PublishBatch => DecodeBatch(r),
            OpCode.CreateStream => new CreateStream(WireStreamSpec.Decode(r)),
            OpCode.UpdateStream => new UpdateStream(WireStreamSpec.Decode(r)),
            OpCode.DeleteStream => new DeleteStream(r.Str()),
            OpCode.StreamInfo => new StreamInfo(r.Str()),
            OpCode.ListStreams => new ListStreams(),
            OpCode.Query => new Query(r.LStr()),
            OpCode.CreateConsumer => new CreateConsumer(r.Bytes().ToArray()),
            OpCode.DeleteConsumer => new DeleteConsumer(r.Str()),
            OpCode.ConsumerInfo => new ConsumerInfo(r.Str()),
            OpCode.ListConsumers => new ListConsumers(r.OptStr()),
            OpCode.SeekConsumer => DecodeSeek(r),
            OpCode.Subscribe => new Subscribe(r.Str(), r.U32()),
            OpCode.Credit => new Credit(r.U32(), r.U32()),
            OpCode.Unsubscribe => new Unsubscribe(r.U32()),
            OpCode.Pull => new Pull(r.Str(), r.U32(), r.U32(), r.U32()),
            OpCode.Ack => new Ack(r.Str(), DecodeOffsets(r)),
            OpCode.Nack => new Nack(r.Str(), r.U64(), r.U32()),
            OpCode.Term => new Term(r.Str(), r.U64(), r.Str()),
            OpCode.InProgress => new InProgress(r.Str(), DecodeOffsets(r)),
            OpCode.Read => new Read(r.Str(), r.U64(), r.U32(), r.U32(), r.U32(), r.Str()),
            OpCode.CorePublish => new CorePublish(r.Str(), r.OptStr(), r.Headers(), r.Bytes()),
            OpCode.CoreSubscribe => new CoreSubscribe(r.Str(), r.OptStr()),
            OpCode.KvCreateBucket => new KvCreateBucket(r.Str(), r.U64(), r.U64(), r.U64()),
            OpCode.KvPut => new KvPut(r.Str(), r.Str(), r.Bytes(), r.OptU64(), r.OptU64()),
            OpCode.KvGet => new KvGet(r.Str(), r.Str(), r.OptU64()),
            OpCode.KvDelete => new KvDelete(r.Str(), r.Str(), r.Bool(), r.OptU64()),
            OpCode.KvKeys => new KvKeys(r.Str(), r.Str()),
            OpCode.KvHistory => new KvHistory(r.Str(), r.Str()),
            _ => throw new ExspeedProtocolException($"opcode 0x{opcode:x} is not a client request"),
        };
        r.Finish();
        return req;
    }

    private static PublishBatch DecodeBatch(WireReader r)
    {
        string stream = r.Str();
        // Smallest record: 2 + 1 + 4 + 2 + 1 = 10 bytes.
        int n = r.Count(10);
        var records = new List<WirePublishRecord>(n);
        for (int i = 0; i < n; i++)
        {
            records.Add(WirePublishRecord.Decode(r));
        }
        return new PublishBatch(stream, records);
    }

    private static SeekConsumer DecodeSeek(WireReader r)
    {
        string consumer = r.Str();
        byte kind = r.U8();
        ulong value = r.U64();
        if (kind > 3)
        {
            throw new ExspeedProtocolException($"unknown seek kind {kind}");
        }
        return new SeekConsumer(consumer, kind, value);
    }
}
