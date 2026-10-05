using System.Text;
using Exspeed.Protocol;

namespace Exspeed.Tests;

/// <summary>Helpers shared by the codec tests.</summary>
internal static class Wire
{
    /// <summary>Hex string (spaces, newlines and <c>|</c> ignored) to bytes.</summary>
    public static byte[] Hex(string s) =>
        Convert.FromHexString(new string(s.Where(c => !char.IsWhiteSpace(c) && c != '|').ToArray()));

    public static string ToHex(ReadOnlySpan<byte> b) => Convert.ToHexString(b).ToLowerInvariant();

    public static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    public static KeyValuePair<string, string> H(string k, string v) => new(k, v);

    public static readonly KeyValuePair<string, string>[] NoHeaders = Array.Empty<KeyValuePair<string, string>>();

    /// <summary>Same as <c>rec(i)</c> in the Rust protocol tests.</summary>
    public static WireRecord Rec(ulong i) => new(
        i,
        1_700_000_000_000_000_000UL + i,
        (ushort)(i % 3),
        $"orders.{i}",
        i % 2 == 0 ? B($"k{i}") : null,
        Enumerable.Repeat((byte)i, (int)(i % 7)).ToArray(),
        new[] { H("h", $"v{i}") });
}

public class ProtocolTests
{
    private static readonly WirePublishRecord Pr = new("a.b", Wire.B("k"), Wire.B("{\"x\":1}"), new[] { Wire.H("h1", "v1") }, "m-1");
    private static readonly WirePublishRecord EmptyPr = new("", null, Array.Empty<byte>(), Wire.NoHeaders, null);
    private static readonly WireStreamSpec Spec = new("s", 1, 2, 3, 4, true, null);

    /// <summary>Same as <c>limited</c> in the Rust round-trip test.</summary>
    private static readonly WireStreamSpec LimitedSpec = new StreamSpec("q")
    {
        MaxMsgs = 10,
        Discard = DiscardPolicy.New,
        MaxMsgsPerSubject = 1,
        AllowMsgTtl = true,
        MsgTtl = TimeSpan.FromMilliseconds(5000),
        AllowDelayed = true,
        Retention = RetentionPolicy.WorkQueue,
    }.ToWire();

    private static readonly ConsumerSpec FullConsumerSpec = new("c", "s")
    {
        FilterSubjects = new[] { "orders.>" },
        Deliver = DeliverPolicy.FromTimeMs(123),
        Ack = AckPolicy.Explicit,
        AckWait = TimeSpan.FromMilliseconds(30000),
        MaxDeliver = 5,
        Backoff = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1000) },
        MaxAckPending = 1000,
        DlqStream = "s-dlq",
        Ephemeral = false,
        DeadLetterExpired = false,
        HeaderMatch = HeaderMatch.All,
        SingleActive = false,
        PriorityWindow = 0,
    };

    internal static readonly Dictionary<string, Request> AllRequests = new()
    {
        ["Connect"] = new Request.Connect("c", "t"),
        ["Connect no token"] = new Request.Connect("c", null),
        ["Ping"] = new Request.Ping(),
        ["Metadata"] = new Request.Metadata(),
        ["Publish"] = new Request.Publish("s", Pr),
        ["PublishBatch"] = new Request.PublishBatch("s", new[] { Pr, EmptyPr }),
        ["CreateStream"] = new Request.CreateStream(Spec),
        ["UpdateStream"] = new Request.UpdateStream(Spec),
        ["DeleteStream"] = new Request.DeleteStream("s"),
        ["StreamInfo"] = new Request.StreamInfo("s"),
        ["ListStreams"] = new Request.ListStreams(),
        ["Query"] = new Request.Query("SELECT 1"),
        ["CreateConsumer"] = new Request.CreateConsumer(FullConsumerSpec.ToWireJson()),
        ["DeleteConsumer"] = new Request.DeleteConsumer("c"),
        ["ConsumerInfo"] = new Request.ConsumerInfo("c"),
        ["ListConsumers all"] = new Request.ListConsumers(null),
        ["ListConsumers stream"] = new Request.ListConsumers("s"),
        ["Seek time"] = new Request.SeekConsumer("c", 3, 9),
        ["Seek latest"] = new Request.SeekConsumer("c", 1, 0),
        ["Subscribe"] = new Request.Subscribe("c", 100),
        ["Credit"] = new Request.Credit(3, 10),
        ["Unsubscribe"] = new Request.Unsubscribe(3),
        ["Pull"] = new Request.Pull("c", 10, 1024, 500),
        ["Ack"] = new Request.Ack("c", new ulong[] { 1, 2, 3 }),
        ["Nack"] = new Request.Nack("c", 4, 100),
        ["Term"] = new Request.Term("c", 5, "bad"),
        ["InProgress"] = new Request.InProgress("c", new ulong[] { 6 }),
        ["Read"] = new Request.Read("s", 7, 100, 1 << 20, 1000, "a.*"),
        ["CreateStream with limits"] = new Request.CreateStream(LimitedSpec),
        ["CorePublish"] = new Request.CorePublish("a.b", "r", new[] { Wire.H("h", "v") }, Wire.B("x")),
        ["CorePublish bare"] = new Request.CorePublish("a", null, Wire.NoHeaders, Array.Empty<byte>()),
        ["CoreSubscribe"] = new Request.CoreSubscribe("a.*", "q"),
        ["CoreSubscribe bare"] = new Request.CoreSubscribe("a.*", null),
        ["KvCreateBucket"] = new Request.KvCreateBucket("b", 5, 1000, 0),
        ["KvPut expecting revision 0"] = new Request.KvPut("b", "k", Wire.B("v"), 0, null),
        ["KvPut with TTL"] = new Request.KvPut("b", "k", Wire.B("v"), null, 500),
        ["KvGet at revision"] = new Request.KvGet("b", "k", 3),
        ["KvGet"] = new Request.KvGet("b", "k", null),
        ["KvDelete purge"] = new Request.KvDelete("b", "k", true, 7),
        ["KvDelete"] = new Request.KvDelete("b", "k", false, null),
        ["KvKeys"] = new Request.KvKeys("b", "a.*"),
        ["KvHistory"] = new Request.KvHistory("b", "k"),
    };

    /// <summary>Byte-exact fixtures derived from (and generated with) the Rust encoder in crates/exspeed-protocol/src/client.rs.</summary>
    public static readonly Dictionary<string, string> RequestFixtures = new()
    {
        ["Connect"] = "0100 63 | 01 0100 74",
        ["Connect no token"] = "0100 63 | 00",
        ["Ping"] = "",
        ["Publish"] = @"0100 73
            0300 612e62
            01 01000000 6b
            07000000 7b2278223a317d
            0100 0200 6831 0200 7631
            01 0300 6d2d31",
        ["PublishBatch"] = @"0100 73 | 02000000
            0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31
            0000 00 00000000 0000 00",
        ["CreateStream"] = "0100 73 0100000000000000 0200000000000000 0300000000000000 0400000000000000 01",
        ["Query"] = "08000000 53454c4543542031",
        ["ListConsumers all"] = "00",
        ["ListConsumers stream"] = "01 0100 73",
        ["Seek time"] = "0100 63 03 0900000000000000",
        ["Seek latest"] = "0100 63 01 0000000000000000",
        ["Subscribe"] = "0100 63 64000000",
        ["Credit"] = "03000000 0a000000",
        ["Unsubscribe"] = "03000000",
        ["Pull"] = "0100 63 0a000000 00040000 f4010000",
        ["Ack"] = "0100 63 03000000 0100000000000000 0200000000000000 0300000000000000",
        ["Nack"] = "0100 63 0400000000000000 64000000",
        ["Term"] = "0100 63 0500000000000000 0300 626164",
        ["Read"] = "0100 73 0700000000000000 64000000 00001000 e8030000 0300 612e2a",
        ["CreateStream with limits"] =
            "0100 71 0000000000000000 0000000000000000 0000000000000000 0000000000000000 00 8d000000 "
            + Wire.ToHex(Wire.B(
                "{\"max_msgs\":10,\"discard\":\"new\",\"max_msgs_per_subject\":1,\"allow_msg_ttl\":true,"
                + "\"msg_ttl_ms\":5000,\"allow_delayed\":true,\"retention\":\"work_queue\"}")),
        ["CorePublish"] = "0300 612e62 | 01 0100 72 | 0100 0100 68 0100 76 | 01000000 78",
        ["CorePublish bare"] = "0100 61 | 00 | 0000 | 00000000",
        ["CoreSubscribe"] = "0300 612e2a 01 0100 71",
        ["CoreSubscribe bare"] = "0300 612e2a 00",
        ["KvCreateBucket"] = "0100 62 0500000000000000 e803000000000000 0000000000000000",
        ["KvPut expecting revision 0"] = "0100 62 0100 6b 01000000 76 01 0000000000000000 00",
        ["KvPut with TTL"] = "0100 62 0100 6b 01000000 76 00 01 f401000000000000",
        ["KvGet at revision"] = "0100 62 0100 6b 01 0300000000000000",
        ["KvGet"] = "0100 62 0100 6b 00",
        ["KvDelete purge"] = "0100 62 0100 6b 01 01 0700000000000000",
        ["KvDelete"] = "0100 62 0100 6b 00 00",
        ["KvKeys"] = "0100 62 0300 612e2a",
        ["KvHistory"] = "0100 62 0100 6b",
    };

    internal static readonly Dictionary<string, Response> AllResponses = new()
    {
        ["Ok"] = new Response.Ok(),
        ["Pong"] = new Response.Pong(),
        ["Error"] = new Response.Error(404, "nope", null),
        ["Error with detail"] = new Response.Error(503, "not leader", Wire.B("{\"leader\":\"h:1\"}")),
        ["ConnectOk"] = new Response.ConnectOk("0.6.0", "n1", "h:5933"),
        ["PublishOk"] = new Response.PublishOk(9, true),
        ["PublishBatchOk"] = new Response.PublishBatchOk(new[] { (1UL, false), (1UL, true) }),
        ["SubscribeOk"] = new Response.SubscribeOk(2),
        ["Deliver many"] = new Response.Deliver(2, new ulong[] { 0, 1, 2, 3, 4 }.Select(Wire.Rec).ToList()),
        ["Deliver"] = new Response.Deliver(2, new[] { Wire.Rec(2) }),
        ["SubscriptionEnded"] = new Response.SubscriptionEnded(2, 404, "consumer deleted"),
        ["Messages"] = new Response.Messages(new[] { Wire.Rec(0) }),
        ["ReadResult many"] = new Response.ReadResult(10, 12, new ulong[] { 0, 1, 2 }.Select(Wire.Rec).ToList()),
        ["ReadResult"] = new Response.ReadResult(10, 12, new[] { Wire.Rec(1) }),
        ["Json"] = Response.Json.Of("{\"a\":1}"),
        ["CoreMsg"] = new Response.CoreMsg(0x80000001, "a", "r", new[] { Wire.H("h", "v") }, Wire.B("x")),
        ["CoreMsg bare"] = new Response.CoreMsg(0x80000002, "a.b", null, Wire.NoHeaders, Array.Empty<byte>()),
    };

    public static readonly Dictionary<string, string> ResponseFixtures = new()
    {
        ["Error"] = "9401 0400 6e6f7065 00",
        ["Error with detail"] = "f701 0a00 6e6f74206c6561646572 01 10000000 7b226c6561646572223a22683a31227d",
        ["ConnectOk"] = "0500 302e362e30 0200 6e31 01 0600 683a35393333",
        ["PublishOk"] = "0900000000000000 01",
        ["PublishBatchOk"] = "02000000 0100000000000000 00 0100000000000000 01",
        ["SubscribeOk"] = "02000000",
        ["SubscriptionEnded"] = "02000000 9401 1000 636f6e73756d65722064656c65746564",
        ["Deliver"] = @"02000000 | 01000000
            36000000 06760cef 0200
            0200000000000000 02002a36fe9c9717
            0800 6f72646572732e32
            01 02000000 6b32
            02000000 0202
            0100 0100 68 0200 7632",
        ["Messages"] = @"01000000
            34000000 b6f9c422 0000
            0000000000000000 00002a36fe9c9717
            0800 6f72646572732e30
            01 02000000 6b30
            00000000
            0100 0100 68 0200 7630",
        ["ReadResult"] = @"0a00000000000000 0c00000000000000 01000000
            2f000000 f194cd5a 0100
            0100000000000000 01002a36fe9c9717
            0800 6f72646572732e31
            00
            01000000 01
            0100 0100 68 0200 7631",
        ["CoreMsg"] = "01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78",
    };

    public static TheoryData<string> RequestNames() => new(AllRequests.Keys);

    public static TheoryData<string> RequestFixtureNames() => new(RequestFixtures.Keys);

    public static TheoryData<string> ResponseNames() => new(AllResponses.Keys);

    public static TheoryData<string> ResponseFixtureNames() => new(ResponseFixtures.Keys);

    // ---- primitives -----------------------------------------------------------

    [Fact]
    public void RoundTripsEveryPrimitive()
    {
        var w = new WireWriter(4); // forces growth
        w.U8(255).U16(65535).U32(0xffffffff).U64(ulong.MaxValue).U64(0);
        w.Str("héllo").LStr("SELECT 'ü'").Bytes(Wire.B("raw"));
        w.OptStr(null).OptStr("x").OptU64(null).OptU64(7).OptBytes(null).OptBytes(Wire.B("b"));
        w.Headers(new[] { Wire.H("k", "v"), Wire.H("k", "v2") });
        var r = new WireReader(w.ToArray());
        Assert.Equal(255, r.U8());
        Assert.Equal(65535, r.U16());
        Assert.Equal(0xffffffffu, r.U32());
        Assert.Equal(ulong.MaxValue, r.U64());
        Assert.Equal(0UL, r.U64());
        Assert.Equal("héllo", r.Str());
        Assert.Equal("SELECT 'ü'", r.LStr());
        Assert.Equal("raw", Encoding.UTF8.GetString(r.Bytes().Span));
        Assert.Null(r.OptStr());
        Assert.Equal("x", r.OptStr());
        Assert.Null(r.OptU64());
        Assert.Equal(7UL, r.OptU64());
        Assert.Null(r.OptBytes());
        Assert.Equal("b", Encoding.UTF8.GetString(r.OptBytes()!));
        Assert.Equal(new[] { Wire.H("k", "v"), Wire.H("k", "v2") }, r.Headers());
        r.Finish();
    }

    [Fact]
    public void EncodesStrAsU16LengthAndUtf8LittleEndian()
    {
        Assert.Equal("0200c3a9", Wire.ToHex(new WireWriter().Str("é").ToArray()));
        Assert.Equal("0807060504030201", Wire.ToHex(new WireWriter().U64(0x0102030405060708).ToArray()));
    }

    [Fact]
    public void RejectsTruncatedTrailingAndMalformedInput()
    {
        Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("01")).U16());
        Assert.Contains("truncated", Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("0500 6162")).Str()).Message);
        Assert.Contains("trailing", Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("00")).Finish()).Message);
        Assert.Contains("option flag", Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("02")).OptStr()).Message);
        Assert.Contains("UTF-8", Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("0200 c328")).Str()).Message);
        Assert.Contains("header count", Assert.Throws<ExspeedProtocolException>(() => new WireReader(Wire.Hex("ffff")).Headers()).Message);
    }

    [Fact]
    public void RejectsValuesThatDoNotFitTheWireTypes()
    {
        Assert.Throws<ExspeedException>(() => new WireWriter().Str(new string('x', 70_000)));
    }

    // ---- frames ----------------------------------------------------------------

    [Fact]
    public void EncodesTheTenByteHeader()
    {
        Assert.Equal(Wire.Hex("02 01 07000000 02000000 aabb"), Frames.Encode(0x01, 7, Wire.Hex("aabb")));
    }

    [Fact]
    public void ParsesFramesSplitAtEveryByteAndSeveralPerChunk()
    {
        var stream = new Response.Pong().ToFrame(1)
            .Concat(new Response.PublishOk(3, false).ToFrame(2))
            .Concat(new Response.Deliver(9, new[] { Wire.Rec(2) }).ToFrame(0))
            .ToArray();
        var p = new FrameParser();
        var frames = new List<Frame>();
        for (int i = 0; i < stream.Length; i++)
        {
            frames.AddRange(p.Push(stream.AsSpan(i, 1)));
        }
        Assert.Equal(
            new[] { ((byte)OpCode.Pong, 1u), ((byte)OpCode.PublishOk, 2u), ((byte)OpCode.Deliver, 0u) },
            frames.Select(f => (f.Opcode, f.CorrelationId)));
        Assert.Equal(0, p.Pending);
        Assert.Equal(3, new FrameParser().Push(stream).Count);
    }

    [Fact]
    public void RejectsABadVersionOrAnOversizeLength()
    {
        Assert.Contains("version", Assert.Throws<ExspeedProtocolException>(() => new FrameParser().Push(Wire.Hex("01 80 00000000 00000000"))).Message);
        var big = new byte[10];
        big[0] = 2;
        big[1] = 0x80;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(big.AsSpan(6), ProtocolConstants.MaxPayloadSize + 1);
        Assert.Contains("too large", Assert.Throws<ExspeedProtocolException>(() => new FrameParser().Push(big)).Message);
        Assert.Contains("too large", Assert.Throws<ExspeedException>(() => Frames.Encode(0x10, 1, new byte[ProtocolConstants.MaxPayloadSize + 1])).Message);
    }

    // ---- requests ----------------------------------------------------------------

    [Theory]
    [MemberData(nameof(RequestNames))]
    public void RequestRoundTrips(string name)
    {
        var req = AllRequests[name];
        var frame = req.ToFrame(42);
        var f = Assert.Single(new FrameParser().Push(frame));
        Assert.Equal((byte)req.Op, f.Opcode);
        Assert.Equal(42u, f.CorrelationId);
        var decoded = Request.Decode(f.Opcode, f.Payload);
        Assert.Equal(req.GetType(), decoded.GetType());
        Assert.Equal(Wire.ToHex(req.EncodePayload()), Wire.ToHex(decoded.EncodePayload()));
    }

    [Theory]
    [MemberData(nameof(RequestFixtureNames))]
    public void RequestMatchesTheRustEncodingByteForByte(string name)
    {
        Assert.Equal(Wire.ToHex(Wire.Hex(RequestFixtures[name])), Wire.ToHex(AllRequests[name].EncodePayload()));
    }

    [Fact]
    public void ConnectFrameMatchesByteForByteHeaderIncluded()
    {
        Assert.Equal(Wire.Hex("02 01 01000000 07000000 | 0100 63 01 0100 74"), new Request.Connect("c", "t").ToFrame(1));
    }

    [Fact]
    public void RejectsTruncatedAndTrailingRequestPayloads()
    {
        Assert.Contains("trailing", Assert.Throws<ExspeedProtocolException>(() => Request.Decode((byte)OpCode.Ping, Wire.Hex("09"))).Message);
        var sub = new Request.Subscribe("c", 1).EncodePayload();
        Assert.Contains("truncated", Assert.Throws<ExspeedProtocolException>(() => Request.Decode((byte)OpCode.Subscribe, sub[..^1])).Message);
    }

    [Fact]
    public void RejectsHostileCountsWithoutAllocating()
    {
        var w = new WireWriter().Str("s").U32(0xffffffff);
        Assert.Contains("exceeds payload", Assert.Throws<ExspeedProtocolException>(() => Request.Decode((byte)OpCode.PublishBatch, w.ToArray())).Message);
    }

    [Fact]
    public void ConsumerSpecJsonMatchesSerdeJson()
    {
        // serde_json::to_vec(&spec) in the Rust round-trip test.
        const string rust =
            "{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"orders.>\"],\"deliver\":{\"from_time\":123},"
            + "\"ack\":\"explicit\",\"ack_wait_ms\":30000,\"max_deliver\":5,\"backoff_ms\":[100,1000],"
            + "\"max_ack_pending\":1000,\"dlq_stream\":\"s-dlq\",\"ephemeral\":false,"
            + "\"dead_letter_expired\":false,\"header_match\":\"all\",\"single_active\":false,\"priority_window\":0}";
        var payload = new Request.CreateConsumer(FullConsumerSpec.ToWireJson()).EncodePayload();
        Assert.Equal((uint)rust.Length, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(payload));
        Assert.Equal(rust, Encoding.UTF8.GetString(payload.AsSpan(4)));
    }

    [Fact]
    public void MapsHeaderFiltersSingleActiveAndPriorityLikeSerde()
    {
        var full = FullConsumerSpec with
        {
            DeadLetterExpired = true,
            FilterHeaders = new Dictionary<string, string> { ["tenant"] = "acme", ["a-first"] = "1" },
            HeaderMatch = HeaderMatch.Any,
            SingleActive = true,
            PriorityWindow = 50,
        };
        Assert.Equal(
            "{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"orders.>\"],\"deliver\":{\"from_time\":123},"
            + "\"ack\":\"explicit\",\"ack_wait_ms\":30000,\"max_deliver\":5,\"backoff_ms\":[100,1000],"
            + "\"max_ack_pending\":1000,\"dlq_stream\":\"s-dlq\",\"ephemeral\":false,\"dead_letter_expired\":true,"
            + "\"filter_headers\":{\"a-first\":\"1\",\"tenant\":\"acme\"},\"header_match\":\"any\",\"single_active\":true,\"priority_window\":50}",
            Encoding.UTF8.GetString(full.ToWireJson()));
        // An empty header filter is left out, as serde does.
        Assert.Equal(
            "{\"name\":\"c\",\"stream\":\"s\"}",
            Encoding.UTF8.GetString(new ConsumerSpec("c", "s") { FilterHeaders = new Dictionary<string, string>() }.ToWireJson()));
    }

    [Fact]
    public void OmitsUnsetConsumerSpecFieldsSoTheServerAppliesItsDefaults()
    {
        Assert.Equal("{\"name\":\"c\",\"stream\":\"s\"}", Encoding.UTF8.GetString(new ConsumerSpec("c", "s").ToWireJson()));
        Assert.Equal(
            "{\"name\":\"c\",\"stream\":\"s\",\"deliver\":{\"from_offset\":5},\"ack\":\"none\"}",
            Encoding.UTF8.GetString(new ConsumerSpec("c", "s") { Deliver = DeliverPolicy.FromOffset(5), Ack = AckPolicy.None }.ToWireJson()));
        Assert.Equal(
            "{\"name\":\"c\",\"stream\":\"s\",\"deliver\":{\"from_time\":42}}",
            Encoding.UTF8.GetString(new ConsumerSpec("c", "s") { Deliver = DeliverPolicy.FromTime(DateTimeOffset.FromUnixTimeMilliseconds(42)) }.ToWireJson()));
        Assert.Equal(
            "{\"name\":\"c\",\"stream\":\"s\",\"deliver\":\"new\"}",
            Encoding.UTF8.GetString(new ConsumerSpec("c", "s") { Deliver = DeliverPolicy.New }.ToWireJson()));
        Assert.Throws<ExspeedException>(() => new ConsumerSpec("", "s").ToWireJson());
        Assert.Throws<ExspeedException>(() => new ConsumerSpec("c", "").ToWireJson());
    }

    // ---- stream limits -------------------------------------------------------------

    [Fact]
    public void SendsNoLimitsTrailerWhenEveryLimitIsAtItsDefault()
    {
        var plain = new StreamSpec("s") { MaxAge = TimeSpan.FromSeconds(1), Discard = DiscardPolicy.Old, Retention = RetentionPolicy.Limits }.ToWire();
        Assert.Null(plain.Limits);
        var payload = new Request.CreateStream(plain).EncodePayload();
        Assert.Equal(
            Wire.ToHex(Wire.Hex("0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00")),
            Wire.ToHex(payload));
        // And an old-style spec without the trailer decodes without limits.
        var decoded = Assert.IsType<Request.CreateStream>(Request.Decode((byte)OpCode.CreateStream, payload));
        Assert.Equal(new WireStreamSpec("s", 1, 0, 0, 0, false, null), decoded.Spec);
    }

    [Fact]
    public void SendsEveryLimitSerdeStyleOnceAnyOneIsSet()
    {
        var one = new StreamSpec("s") { AllowDelayed = true }.ToWire();
        Assert.Equal(
            "{\"max_msgs\":0,\"discard\":\"old\",\"max_msgs_per_subject\":0,\"allow_msg_ttl\":false,"
            + "\"msg_ttl_ms\":0,\"allow_delayed\":true,\"retention\":\"limits\"}",
            Encoding.UTF8.GetString(one.Limits!.ToJson()));
        var specs = new[]
        {
            new StreamSpec("s") { MaxMsgs = 1 },
            new StreamSpec("s") { Discard = DiscardPolicy.New },
            new StreamSpec("s") { MaxMsgsPerSubject = 2 },
            new StreamSpec("s") { AllowMsgTtl = true },
            new StreamSpec("s") { MsgTtl = TimeSpan.FromMilliseconds(3) },
            new StreamSpec("s") { Retention = RetentionPolicy.Interest },
        };
        foreach (var s in specs)
        {
            Assert.NotNull(s.ToWire().Limits);
        }
    }

    [Fact]
    public void AddsCaptureSubjectsOnlyWhenThereAreSome()
    {
        Assert.Null(new StreamSpec("s") { CaptureSubjects = Array.Empty<string>() }.ToWire().Limits);
        var c = new StreamSpec("s") { CaptureSubjects = new[] { "orders.>" } }.ToWire();
        // The JSON pinned by capture_subjects_are_serialized_only_when_set in crates/exspeed-common/src/limits.rs.
        Assert.Equal(
            "{\"max_msgs\":0,\"discard\":\"old\",\"max_msgs_per_subject\":0,\"allow_msg_ttl\":false,"
            + "\"msg_ttl_ms\":0,\"allow_delayed\":false,\"retention\":\"limits\",\"capture_subjects\":[\"orders.>\"]}",
            Encoding.UTF8.GetString(c.Limits!.ToJson()));
        var payload = new Request.CreateStream(c).EncodePayload();
        var decoded = Assert.IsType<Request.CreateStream>(Request.Decode((byte)OpCode.CreateStream, payload));
        Assert.Equal(new[] { "orders.>" }, decoded.Spec.Limits!.CaptureSubjects);
        Assert.Equal(Wire.ToHex(payload), Wire.ToHex(decoded.EncodePayload()));
    }

    [Fact]
    public void ConvertsStreamDurationsToWholeUnits()
    {
        var w = new StreamSpec("s") { MaxAge = TimeSpan.FromMilliseconds(1500), DedupWindow = TimeSpan.FromMinutes(5) }.ToWire();
        Assert.Equal(2UL, w.MaxAgeSecs);
        Assert.Equal(300UL, w.DedupWindowSecs);
        Assert.Throws<ExspeedException>(() => new StreamSpec("").ToWire());
        Assert.Throws<ExspeedException>(() => new StreamSpec("s") { MaxAge = TimeSpan.FromSeconds(-1) }.ToWire());
    }

    // ---- publish options --------------------------------------------------------------

    [Fact]
    public void PublishOptionsBecomeTheSameHeadersAsTheRustBuilders()
    {
        var r = new PublishRecord("a", "v")
        {
            Headers = new[] { Wire.H("trace-id", "t") },
            Ttl = TimeSpan.FromMilliseconds(500),
            Delay = TimeSpan.FromSeconds(2),
            DeliverAt = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            Priority = 7,
        }.ToWire();
        // PublishRecord::new("a", "v").ttl(500ms).delay(2s).deliver_at(1700000000000).priority(7)
        Assert.Equal(
            new[]
            {
                Wire.H("trace-id", "t"),
                Wire.H("exspeed-ttl", "500ms"),
                Wire.H("exspeed-delay", "2000ms"),
                Wire.H("exspeed-deliver-at", "1700000000000"),
                Wire.H("exspeed-priority", "7"),
            },
            r.Headers);
    }

    [Fact]
    public void PublishOptionsRoundUpAndRejectBadValues()
    {
        var r = new PublishRecord("a", "v") { Ttl = TimeSpan.FromTicks(2000), Delay = TimeSpan.Zero, DeliverAt = DateTimeOffset.FromUnixTimeMilliseconds(42) }.ToWire();
        Assert.Equal(new[] { Wire.H("exspeed-ttl", "1ms"), Wire.H("exspeed-delay", "0ms"), Wire.H("exspeed-deliver-at", "42") }, r.Headers);
        Assert.Equal(new[] { Wire.H("exspeed-ttl", "1ms") }, new PublishRecord("a", "v") { Ttl = TimeSpan.Zero }.ToWire().Headers);
        Assert.Throws<ExspeedException>(() => new PublishRecord("a", "v") { Delay = TimeSpan.FromMilliseconds(-1) }.ToWire());
        Assert.Throws<ExspeedException>(() => new PublishRecord("a", "v") { Priority = 10 }.ToWire());
        Assert.Throws<ExspeedException>(() => new PublishRecord("a", "v") { Priority = -1 }.ToWire());
        Assert.Throws<ExspeedException>(() => new PublishRecord("a", "v") { DeliverAt = DateTimeOffset.FromUnixTimeMilliseconds(-5) }.ToWire());
    }

    [Fact]
    public void PublishRecordValuesBytesTextAndJson()
    {
        Assert.Equal("hé", Encoding.UTF8.GetString(new PublishRecord("a", "hé").Value.Span));
        Assert.Equal(new byte[] { 1, 2 }, new PublishRecord("a", new byte[] { 1, 2 }).Value.ToArray());
        Assert.Equal("{\"id\":1}", Encoding.UTF8.GetString(PublishRecord.Json("a", new { id = 1 }).Value.Span));
    }

    // ---- responses -------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(ResponseNames))]
    public void ResponseRoundTrips(string name)
    {
        var resp = AllResponses[name];
        var f = Assert.Single(new FrameParser().Push(resp.ToFrame(7)));
        Assert.Equal((byte)resp.Op, f.Opcode);
        var decoded = Response.Decode(f.Opcode, f.Payload);
        Assert.Equal(resp.GetType(), decoded.GetType());
        Assert.Equal(Wire.ToHex(resp.EncodePayload()), Wire.ToHex(decoded.EncodePayload()));
    }

    [Theory]
    [MemberData(nameof(ResponseFixtureNames))]
    public void ResponseDecodesFromTheRustEncoding(string name)
    {
        var resp = AllResponses[name];
        var bytes = Wire.Hex(ResponseFixtures[name]);
        var decoded = Response.Decode((byte)resp.Op, bytes);
        Assert.Equal(Wire.ToHex(bytes), Wire.ToHex(decoded.EncodePayload()));
        Assert.Equal(Wire.ToHex(bytes), Wire.ToHex(resp.EncodePayload()));
    }

    [Fact]
    public void DecodesRecordFieldsFromTheRustEncoding()
    {
        var d = Assert.IsType<Response.Deliver>(Response.Decode((byte)OpCode.Deliver, Wire.Hex(ResponseFixtures["Deliver"])));
        Assert.Equal(2u, d.SubId);
        var r = Assert.Single(d.Records);
        Assert.Equal(2UL, r.Offset);
        Assert.Equal(1_700_000_000_000_000_002UL, r.TimestampNs);
        Assert.Equal(2, r.DeliveryCount);
        Assert.Equal("orders.2", r.Subject);
        Assert.Equal("k2", Encoding.UTF8.GetString(r.Key!));
        Assert.Equal(new byte[] { 2, 2 }, r.Value.ToArray());
        Assert.Equal(new[] { Wire.H("h", "v2") }, r.Headers);

        var e = Assert.IsType<Response.Error>(Response.Decode((byte)OpCode.Error, Wire.Hex(ResponseFixtures["Error with detail"])));
        var ex = e.ToException();
        Assert.Equal(503, ex.Code);
        Assert.Equal("not leader", ex.Message);
        Assert.Equal("h:1", ex.LeaderHint);
    }

    [Fact]
    public void RejectsHostileRecordCountsAndUnknownOpcodes()
    {
        var w = new WireWriter().U64(0).U64(0).U32(0xffffffff);
        Assert.Contains("exceeds payload", Assert.Throws<ExspeedProtocolException>(() => Response.Decode((byte)OpCode.ReadResult, w.ToArray())).Message);
        Assert.Contains("not a server response", Assert.Throws<ExspeedProtocolException>(() => Response.Decode((byte)OpCode.Publish, Array.Empty<byte>())).Message);
        Assert.Contains("not a client request", Assert.Throws<ExspeedProtocolException>(() => Request.Decode((byte)OpCode.Ok, Array.Empty<byte>())).Message);
    }

    // ---- records ---------------------------------------------------------------------

    [Fact]
    public void RecordsCarryACrc32CThatIgnoresDeliveryCount()
    {
        var enc = Wire.Rec(2).Encode();
        Assert.Equal(0x36 + 4, enc.Length);
        Assert.True(WireRecord.VerifyCrc(enc));
        // The server patches delivery_count (bytes 8..10) in place.
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(enc.AsSpan(8), 7);
        Assert.True(WireRecord.VerifyCrc(enc));
        var payload = Wire.Hex("01000000").Concat(enc).ToArray();
        var m = Assert.IsType<Response.Messages>(Response.Decode((byte)OpCode.Messages, payload));
        Assert.Equal(7, m.Records[0].DeliveryCount);
        enc[^1] ^= 1;
        Assert.False(WireRecord.VerifyCrc(enc));
        // Decoding verifies the CRC.
        var bad = Wire.Hex("01000000").Concat(enc).ToArray();
        Assert.Contains("CRC", Assert.Throws<ExspeedProtocolException>(() => Response.Decode((byte)OpCode.Messages, bad)).Message);
    }

    [Fact]
    public void Crc32CMatchesTheStandardCheckValue()
    {
        Assert.Equal(0xe3069283u, Crc32C.Compute(Wire.B("123456789")));
        Assert.Equal(0xe3069283u, Crc32C.ComputeSoftware(Wire.B("123456789")));
        var data = Enumerable.Range(0, 1000).Select(i => (byte)(i * 7)).ToArray();
        for (int n = 0; n < 40; n++)
        {
            Assert.Equal(Crc32C.ComputeSoftware(data.AsSpan(0, n * 17)), Crc32C.Compute(data.AsSpan(0, n * 17)));
        }
    }

    [Fact]
    public void RejectsARecordLengthThatDisagreesWithItsContents()
    {
        var enc = Wire.Rec(1).Encode();
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(enc, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(enc) + 1);
        var payload = Wire.Hex("01000000").Concat(enc).Concat(Wire.Hex("00")).ToArray();
        Assert.Throws<ExspeedProtocolException>(() => Response.Decode((byte)OpCode.Messages, payload));
        var tiny = Wire.Hex("01000000 05000000").Concat(new byte[40]).ToArray();
        Assert.Contains("too small", Assert.Throws<ExspeedProtocolException>(() => Response.Decode((byte)OpCode.Messages, tiny)).Message);
    }
}

public class MsgIdTests
{
    [Fact]
    public void MatchesUuidV7Format()
    {
        Assert.Matches("^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", MsgId.New());
        Assert.Equal(36, MsgId.New().Length);
    }

    [Fact]
    public void ReturnsUniqueValues()
    {
        Assert.Equal(1000, Enumerable.Range(0, 1000).Select(_ => MsgId.New()).Distinct().Count());
    }

    [Fact]
    public async Task LaterIdsSortAfterEarlierOnes()
    {
        var a = MsgId.New();
        await Task.Delay(5);
        Assert.True(string.CompareOrdinal(a, MsgId.New()) < 0);
    }

    [Fact]
    public void EncodesTheTimestampInTheFirst48Bits()
    {
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var id = MsgId.New();
        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        long ts = Convert.ToInt64(id[..8] + id[9..13], 16);
        Assert.InRange(ts, before, after);
    }
}
