namespace Exspeed.Protocol;

/// <summary>
/// Every response and push of client protocol v2, mirroring <c>Response</c> in
/// <c>crates/exspeed-protocol/src/client.rs</c>. Encoding (what the server does) is used by the unit
/// tests' fake server.
/// </summary>
internal abstract record Response
{
    public abstract OpCode Op { get; }

    public string Name => GetType().Name;

    public abstract void Encode(WireWriter w);

    public byte[] EncodePayload()
    {
        var w = new WireWriter();
        Encode(w);
        return w.ToArray();
    }

    public byte[] ToFrame(uint correlationId)
    {
        var w = WireWriter.ForFrame();
        Encode(w);
        return w.FinishFrame(Op, correlationId);
    }

    public sealed record Ok : Response
    {
        public override OpCode Op => OpCode.Ok;
        public override void Encode(WireWriter w) { }
    }

    public sealed record Pong : Response
    {
        public override OpCode Op => OpCode.Pong;
        public override void Encode(WireWriter w) { }
    }

    /// <summary><paramref name="Detail"/> is raw JSON bytes when present.</summary>
    public sealed record Error(ushort Code, string Message, byte[]? Detail) : Response
    {
        public override OpCode Op => OpCode.Error;
        public override void Encode(WireWriter w) => w.U16(Code).Str(Message).OptBytes(Detail);
        public ExspeedServerException ToException() => ExspeedServerException.FromWire(Code, Message, Detail);
    }

    public sealed record ConnectOk(string ServerVersion, string NodeId, string? Leader) : Response
    {
        public override OpCode Op => OpCode.ConnectOk;
        public override void Encode(WireWriter w) => w.Str(ServerVersion).Str(NodeId).OptStr(Leader);
    }

    public sealed record PublishOk(ulong Offset, bool Duplicate) : Response
    {
        public override OpCode Op => OpCode.PublishOk;
        public override void Encode(WireWriter w) => w.U64(Offset).Bool(Duplicate);
    }

    public sealed record PublishBatchOk(IReadOnlyList<(ulong Offset, bool Duplicate)> Results) : Response
    {
        public override OpCode Op => OpCode.PublishBatchOk;
        public override void Encode(WireWriter w)
        {
            w.U32((uint)Results.Count);
            foreach (var (o, d) in Results)
            {
                w.U64(o).Bool(d);
            }
        }
    }

    public sealed record SubscribeOk(uint SubId) : Response
    {
        public override OpCode Op => OpCode.SubscribeOk;
        public override void Encode(WireWriter w) => w.U32(SubId);
    }

    /// <summary>Push (correlation id 0).</summary>
    public sealed record Deliver(uint SubId, IReadOnlyList<WireRecord> Records) : Response
    {
        public override OpCode Op => OpCode.Deliver;
        public override void Encode(WireWriter w)
        {
            w.U32(SubId);
            WireRecord.EncodeList(w, Records);
        }
    }

    /// <summary>Push (correlation id 0).</summary>
    public sealed record SubscriptionEnded(uint SubId, ushort Code, string Message) : Response
    {
        public override OpCode Op => OpCode.SubscriptionEnded;
        public override void Encode(WireWriter w) => w.U32(SubId).U16(Code).Str(Message);
    }

    public sealed record Messages(IReadOnlyList<WireRecord> Records) : Response
    {
        public override OpCode Op => OpCode.Messages;
        public override void Encode(WireWriter w) => WireRecord.EncodeList(w, Records);
    }

    public sealed record ReadResult(ulong NextOffset, ulong HighWatermark, IReadOnlyList<WireRecord> Records) : Response
    {
        public override OpCode Op => OpCode.ReadResult;
        public override void Encode(WireWriter w)
        {
            w.U64(NextOffset).U64(HighWatermark);
            WireRecord.EncodeList(w, Records);
        }
    }

    /// <summary>Raw UTF-8 JSON.</summary>
    public sealed record Json(ReadOnlyMemory<byte> Body) : Response
    {
        public override OpCode Op => OpCode.Json;
        public override void Encode(WireWriter w) => w.Raw(Body.Span);
        public static Json Of(string json) => new(System.Text.Encoding.UTF8.GetBytes(json));
    }

    /// <summary>Push of a core message for a <c>CoreSubscribe</c> (correlation id 0).</summary>
    public sealed record CoreMsg(
        uint SubId,
        string Subject,
        string? ReplyTo,
        IReadOnlyList<KeyValuePair<string, string>> Headers,
        ReadOnlyMemory<byte> Value) : Response
    {
        public override OpCode Op => OpCode.CoreMsg;
        public override void Encode(WireWriter w) =>
            w.U32(SubId).Str(Subject).OptStr(ReplyTo).Headers(Headers).Bytes(Value.Span);
    }

    /// <summary>Decode a response or push payload.</summary>
    public static Response Decode(byte opcode, ReadOnlyMemory<byte> payload)
    {
        if ((OpCode)opcode == OpCode.Json)
        {
            return new Json(payload);
        }
        var r = new WireReader(payload);
        Response resp = (OpCode)opcode switch
        {
            OpCode.Ok => new Ok(),
            OpCode.Pong => new Pong(),
            OpCode.Error => new Error(r.U16(), r.Str(), r.OptBytes()),
            OpCode.ConnectOk => new ConnectOk(r.Str(), r.Str(), r.OptStr()),
            OpCode.PublishOk => new PublishOk(r.U64(), r.Bool()),
            OpCode.PublishBatchOk => DecodeBatchOk(r),
            OpCode.SubscribeOk => new SubscribeOk(r.U32()),
            OpCode.Deliver => new Deliver(r.U32(), WireRecord.DecodeList(r)),
            OpCode.SubscriptionEnded => new SubscriptionEnded(r.U32(), r.U16(), r.Str()),
            OpCode.Messages => new Messages(WireRecord.DecodeList(r)),
            OpCode.ReadResult => new ReadResult(r.U64(), r.U64(), WireRecord.DecodeList(r)),
            OpCode.CoreMsg => new CoreMsg(r.U32(), r.Str(), r.OptStr(), r.Headers(), r.Bytes()),
            _ => throw new ExspeedProtocolException($"opcode 0x{opcode:x} is not a server response"),
        };
        r.Finish();
        return resp;
    }

    private static PublishBatchOk DecodeBatchOk(WireReader r)
    {
        int n = r.Count(9);
        var list = new (ulong, bool)[n];
        for (int i = 0; i < n; i++)
        {
            list[i] = (r.U64(), r.Bool());
        }
        return new PublishBatchOk(list);
    }
}
