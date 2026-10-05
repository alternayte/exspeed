using System.Text;
using Exspeed.Protocol;

namespace Exspeed.Tests;

/// <summary>Connection logic against the scriptable fake server: handshake, correlation, pushes, reconnect.</summary>
public class ClientTests
{
    internal static WireRecord Rec(ulong offset, string? value = null) => new(
        offset,
        1_700_000_000_000_000_000UL + offset,
        1,
        "orders.placed",
        null,
        Wire.B(value ?? $"v{offset}"),
        new[] { Wire.H("h", "1") });

    internal static ExspeedClientOptions Opts(FakeServer s) =>
        new() { Port = s.Port, Keepalive = TimeSpan.Zero, Reconnect = null };

    internal static ReconnectOptions FastReconnect(int maxAttempts = int.MaxValue) =>
        new() { InitialDelay = TimeSpan.FromMilliseconds(10), MaxDelay = TimeSpan.FromMilliseconds(20), MaxAttempts = maxAttempts };

    // ---- handshake --------------------------------------------------------------

    [Fact]
    public async Task SendsClientIdAndTokenAndExposesServerInfo()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Token = "secret", ClientId = "unit" });
        var first = server.Last.Received[0];
        Assert.Equal(new Request.Connect("unit", "secret"), first.Req);
        Assert.NotEqual(0u, first.Corr);
        Assert.Equal(new ServerInfo("test", "n1", null), c.ServerInfo);
        Assert.True(c.IsConnected);
    }

    [Fact]
    public async Task RejectsWithServerException401WhenTheTokenIsRefused()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.Connect)
            {
                return false;
            }
            conn.Reply(corr, new Response.Error(401, "unauthorized", null));
            conn.Destroy();
            return true;
        });
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => ExspeedClient.ConnectAsync(Opts(server) with { Token = "bad" }));
        Assert.Equal(401, err.Code);
    }

    [Fact]
    public async Task FailsWithConnectionExceptionWhenNothingListens()
    {
        var s = FakeServer.Start();
        int port = s.Port;
        await s.DisposeAsync();
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => ExspeedClient.ConnectAsync(new ExspeedClientOptions { Port = port, Reconnect = null }));
    }

    [Fact]
    public async Task HonoursCancellationWhileConnecting()
    {
        await using var server = FakeServer.Start((_, _, req) => req is Request.Connect); // never answers
        using var cts = new CancellationTokenSource(100);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ExspeedClient.ConnectAsync(Opts(server), cts.Token));
    }

    // ---- requests ---------------------------------------------------------------

    [Fact]
    public async Task MatchesOutOfOrderResponsesByCorrelationId()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        server.Handler = (_, _, _) => true; // answer manually
        var a = c.PublishAsync("s", "a", "1");
        var b = c.PublishAsync("s", "b", "2");
        await FakeServer.Until(() => server.Last.Of<Request.Publish>().Count == 2);
        var pubs = server.Last.Of<Request.Publish>();
        server.Last.Reply(pubs[1].Corr, new Response.PublishOk(2, false));
        server.Last.Reply(pubs[0].Corr, new Response.PublishOk(1, true));
        Assert.Equal(new PublishResult(1, true), await a);
        Assert.Equal(new PublishResult(2, false), await b);
    }

    [Fact]
    public async Task EncodesValuesKeysHeadersAndMsgIds()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.PublishBatch pb)
            {
                conn.Reply(corr, new Response.PublishBatchOk(pb.Records.Select((_, i) => ((ulong)i, false)).ToList()));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var results = await c.PublishBatchAsync("s", new PublishRecord[]
        {
            new PublishRecord("a", new byte[] { 1, 2 }),
            new PublishRecord("a", "hé"),
            PublishRecord.Json("a", new { id = 1 }) with { Key = Wire.B("k"), Headers = new[] { Wire.H("x", "y") }, MsgId = "m1" },
        });
        Assert.Equal(new ulong[] { 0, 1, 2 }, results.Select(r => r.Offset));
        var recs = server.Last.Of<Request.PublishBatch>()[0].Req.Records;
        Assert.Equal(new byte[] { 1, 2 }, recs[0].Value.ToArray());
        Assert.Null(recs[0].Key);
        Assert.Null(recs[0].MsgId);
        Assert.Equal("hé", Encoding.UTF8.GetString(recs[1].Value.Span));
        Assert.Equal("{\"id\":1}", Encoding.UTF8.GetString(recs[2].Value.Span));
        Assert.Equal("k", Encoding.UTF8.GetString(recs[2].Key!));
        Assert.Equal(new[] { Wire.H("x", "y") }, recs[2].Headers);
        Assert.Equal("m1", recs[2].MsgId);
        Assert.Empty(await c.PublishBatchAsync("s", Array.Empty<PublishRecord>()));
    }

    [Fact]
    public async Task SurfacesCodeMessageDetailAndLeaderHintOfServerErrors()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.StreamInfo)
            {
                conn.Reply(corr, new Response.Error(503, "not the leader", Wire.B("{\"leader\":\"h2:5933\"}")));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => c.StreamInfoAsync("s"));
        Assert.Equal(503, err.Code);
        Assert.Equal("not the leader", err.Message);
        Assert.Equal("{\"leader\":\"h2:5933\"}", err.DetailJson);
        Assert.Equal("h2:5933", err.Detail!.Value.GetProperty("leader").GetString());
        Assert.Equal("h2:5933", err.LeaderHint);
    }

    [Fact]
    public async Task TimesOutRequestsTheServerNeverAnswers()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { RequestTimeout = TimeSpan.FromMilliseconds(100) });
        server.Handler = (_, _, _) => true;
        var err = await Assert.ThrowsAsync<ExspeedTimeoutException>(() => c.MetadataAsync());
        Assert.Contains("Metadata timed out", err.Message);
    }

    [Fact]
    public async Task CancelsARequest()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        server.Handler = (_, _, _) => true;
        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => c.MetadataAsync(cts.Token));
    }

    [Fact]
    public async Task GivesPullAndReadExtraTimeForTheirServerSideWait()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Pull)
            {
                _ = Task.Delay(150).ContinueWith(_ => conn.Reply(corr, new Response.Messages(new[] { Rec(3) })));
                return true;
            }
            if (req is Request.Read)
            {
                _ = Task.Delay(150).ContinueWith(_ => conn.Reply(corr, new Response.ReadResult(5, 5, new[] { Rec(4) })));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { RequestTimeout = TimeSpan.FromMilliseconds(50) });
        var msgs = await c.PullAsync("c", new PullOptions { Expires = TimeSpan.FromMilliseconds(200) });
        Assert.Equal(new ulong[] { 3 }, msgs.Select(m => m.Offset));
        var pull = server.Last.Of<Request.Pull>()[0].Req;
        Assert.Equal(new Request.Pull("c", 100, 0, 200), pull);
        var read = await c.ReadAsync("s", new ReadOptions { From = 4, Wait = TimeSpan.FromMilliseconds(200), Filter = "orders.*", MaxRecords = 7 });
        Assert.Equal(new ulong[] { 4 }, read.Records.Select(r => r.Offset));
        Assert.Equal(5UL, read.NextOffset);
        Assert.Equal(new Request.Read("s", 4, 7, 0, 200, "orders.*"), server.Last.Of<Request.Read>()[0].Req);
    }

    [Fact]
    public async Task ParsesJsonReplies()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Metadata:
                    conn.Reply(corr, Response.Json.Of("{\"node_id\":\"n1\",\"is_leader\":true,\"leader\":null,\"server_version\":\"x\"}"));
                    return true;
                case Request.StreamInfo:
                    conn.Reply(corr, Response.Json.Of(
                        "{\"name\":\"s\",\"earliest_offset\":2,\"next_offset\":9,\"records\":7,\"internal\":false,"
                        + "\"config\":{\"max_age_secs\":60,\"max_bytes\":10,\"dedup_window_secs\":300,\"dedup_max_entries\":5,"
                        + "\"compaction\":true,\"tombstone_retention_secs\":86400,\"max_msgs\":3,\"discard\":\"new\","
                        + "\"max_msgs_per_subject\":1,\"allow_msg_ttl\":true,\"msg_ttl_ms\":1500,\"allow_delayed\":true,\"retention\":\"work_queue\",\"capture_subjects\":[\"cap.>\"]}}"));
                    return true;
                case Request.ListStreams:
                    conn.Reply(corr, Response.Json.Of("[{\"name\":\"a\",\"config\":{}},{\"name\":\"b\",\"config\":{}}]"));
                    return true;
                case Request.Query:
                    conn.Reply(corr, Response.Json.Of("{\"columns\":[\"cnt\",\"r\"],\"rows\":[[3,\"eu\"]],\"row_count\":1,\"execution_time_ms\":2.5,\"truncated\":false}"));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        Assert.Equal(new ServerMetadata("n1", true, null, "x"), await c.MetadataAsync());

        var info = await c.StreamInfoAsync("s");
        Assert.Equal(("s", 2UL, 9UL, 7UL, false), (info.Name, info.EarliestOffset, info.NextOffset, info.Records, info.Internal));
        Assert.Equal(
            new StreamConfig
            {
                MaxAge = TimeSpan.FromSeconds(60),
                MaxBytes = 10,
                DedupWindow = TimeSpan.FromSeconds(300),
                DedupMaxEntries = 5,
                Compaction = true,
                MaxMsgs = 3,
                Discard = DiscardPolicy.New,
                MaxMsgsPerSubject = 1,
                AllowMsgTtl = true,
                MsgTtl = TimeSpan.FromMilliseconds(1500),
                AllowDelayed = true,
                Retention = RetentionPolicy.WorkQueue,
            },
            info.Config with { CaptureSubjects = Array.Empty<string>() });
        Assert.Equal(new[] { "cap.>" }, info.Config.CaptureSubjects);
        Assert.Equal(86400, info.Raw.GetProperty("config").GetProperty("tombstone_retention_secs").GetInt32());
        Assert.Equal(new[] { "a", "b" }, (await c.ListStreamsAsync()).Select(s => s.Name));

        var q = await c.QueryAsync("SELECT 1");
        Assert.Equal(new[] { "cnt", "r" }, q.Columns);
        Assert.Equal(3, q.Rows[0][0].GetInt32());
        Assert.Equal("eu", q.Rows[0][1].GetString());
        Assert.Equal((1UL, 2.5, false), (q.RowCount, q.ExecutionTimeMs, q.Truncated));
    }

    [Fact]
    public async Task ReportsBadJsonAsAProtocolError()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Metadata)
            {
                conn.Reply(corr, Response.Json.Of("{nope"));
                return true;
            }
            if (req is Request.ListStreams)
            {
                conn.Reply(corr, new Response.Ok());
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        Assert.Contains("bad JSON", (await Assert.ThrowsAsync<ExspeedProtocolException>(() => c.MetadataAsync())).Message);
        Assert.Contains("unexpected reply to ListStreams: Ok", (await Assert.ThrowsAsync<ExspeedProtocolException>(() => c.ListStreamsAsync())).Message);
    }

    [Fact]
    public async Task EncodesStreamAndConsumerAdminRequests()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.CreateStream or Request.UpdateStream or Request.DeleteStream or Request.DeleteConsumer or Request.SeekConsumer:
                case Request.Ack or Request.Nack or Request.Term or Request.InProgress:
                    conn.Reply(corr, new Response.Ok());
                    return true;
                case Request.ListConsumers:
                    conn.Reply(corr, Response.Json.Of("[]"));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        await c.CreateStreamAsync("plain");
        await c.CreateStreamAsync(new StreamSpec("q") { MaxAge = TimeSpan.FromHours(1), AllowDelayed = true });
        await c.UpdateStreamAsync(new StreamSpec("q") { MaxBytes = 1024 });
        await c.DeleteStreamAsync("q");
        await c.DeleteConsumerAsync("c");
        await c.SeekAsync("c", SeekTarget.Earliest);
        await c.SeekAsync("c", SeekTarget.Latest);
        await c.SeekAsync("c", SeekTarget.Offset(1000));
        await c.SeekAsync("c", SeekTarget.Time(DateTimeOffset.FromUnixTimeMilliseconds(1234)));
        await c.AckAsync("c", new ulong[] { 1, 2 });
        await c.NackAsync("c", 3);
        await c.NackAsync("c", 3, TimeSpan.FromMilliseconds(250));
        await c.TermAsync("c", 4, "bad");
        await c.InProgressAsync("c", new ulong[] { 5 });
        Assert.Empty(await c.ListConsumersAsync("q"));
        await c.ListConsumersAsync();

        var reqs = server.Last.Received.Skip(1).Select(r => r.Req).ToList();
        Assert.Equal(new WireStreamSpec("plain", 0, 0, 0, 0, false, null), ((Request.CreateStream)reqs[0]).Spec);
        var q = ((Request.CreateStream)reqs[1]).Spec;
        Assert.Equal(3600UL, q.MaxAgeSecs);
        Assert.True(q.Limits!.AllowDelayed);
        Assert.Equal(1024UL, ((Request.UpdateStream)reqs[2]).Spec.MaxBytes);
        Assert.Equal(new Request.DeleteStream("q"), reqs[3]);
        Assert.Equal(new Request.DeleteConsumer("c"), reqs[4]);
        Assert.Equal(new Request.SeekConsumer("c", 0, 0), reqs[5]);
        Assert.Equal(new Request.SeekConsumer("c", 1, 0), reqs[6]);
        Assert.Equal(new Request.SeekConsumer("c", 2, 1000), reqs[7]);
        Assert.Equal(new Request.SeekConsumer("c", 3, 1234), reqs[8]);
        Assert.Equal(new ulong[] { 1, 2 }, ((Request.Ack)reqs[9]).Offsets);
        Assert.Equal(new Request.Nack("c", 3, 0), reqs[10]);
        Assert.Equal(new Request.Nack("c", 3, 250), reqs[11]);
        Assert.Equal(new Request.Term("c", 4, "bad"), reqs[12]);
        Assert.Equal(new ulong[] { 5 }, ((Request.InProgress)reqs[13]).Offsets);
        Assert.Equal(new Request.ListConsumers("q"), reqs[14]);
        Assert.Equal(new Request.ListConsumers(null), reqs[15]);
    }

    [Fact]
    public async Task FailsPendingRequestsWithConnectionExceptionWhenTheConnectionDrops()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        server.Handler = (_, _, _) => true;
        var p = c.MetadataAsync();
        await FakeServer.Until(() => server.Last.Of<Request.Metadata>().Count == 1);
        server.Last.Destroy();
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => p);
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => c.PingAsync());
        Assert.False(c.IsConnected);
    }

    // ---- subscriptions ------------------------------------------------------------

    [Fact]
    public async Task KeepsDeliverFramesThatArriveInTheSameChunkAsSubscribeOk()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.Subscribe)
            {
                return false;
            }
            conn.ReplyMany((corr, new Response.SubscribeOk(5)), (0, new Response.Deliver(5, new[] { Rec(0), Rec(1) })));
            return true;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("billing", new SubscribeOptions { Window = 10 });
        Assert.Equal(5u, sub.Id);
        var m0 = await sub.NextAsync(TimeSpan.FromSeconds(1));
        var m1 = await sub.NextAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(new ulong?[] { 0, 1 }, new[] { m0?.Offset, m1?.Offset });
        Assert.Equal("v0", m0!.Text());
        Assert.Equal("1", m0.Header("h"));
        Assert.Null(m0.Header("missing"));
        Assert.Equal(1, m0.DeliveryCount);
        Assert.Equal("billing", m0.Consumer);
        Assert.Equal("orders.placed", m0.Subject);
        Assert.Null(m0.Key);
        Assert.Equal(1_700_000_000_000_000_000UL, m0.TimestampNs);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), m0.Timestamp);
    }

    [Fact]
    public async Task ReturnsCreditInBatchesOfHalfTheWindowAsMessagesAreTaken()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.Reply(corr, new Response.SubscribeOk(9));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c", new SubscribeOptions { Window = 4 });
        var s = server.Last.Of<Request.Subscribe>()[0];
        Assert.Equal(4u, s.Req.Credits);
        Assert.NotEqual(0u, s.Corr);
        server.Last.Reply(0, new Response.Deliver(9, new ulong[] { 0, 1, 2, 3 }.Select(i => Rec(i)).ToList()));
        await sub.NextAsync();
        await Task.Delay(50);
        Assert.Empty(server.Last.Of<Request.Credit>());
        await sub.NextAsync();
        await FakeServer.Until(() => server.Last.Of<Request.Credit>().Count == 1);
        Assert.Equal((0u, new Request.Credit(9, 2)), server.Last.Of<Request.Credit>()[0]);
        await sub.NextAsync();
        await sub.NextAsync();
        await FakeServer.Until(() => server.Last.Of<Request.Credit>().Count == 2);
    }

    [Fact]
    public async Task AcksFireAndForgetAndSettlesWithNackTermInProgress()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Subscribe:
                    conn.ReplyMany((corr, new Response.SubscribeOk(1)), (0, new Response.Deliver(1, new[] { Rec(7) })));
                    return true;
                case Request.Nack or Request.Term or Request.InProgress:
                    conn.Reply(corr, new Response.Ok());
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        var m = (await sub.NextAsync())!;
        m.Ack();
        await m.NackAsync(TimeSpan.FromMilliseconds(250));
        await m.TermAsync("poison");
        await m.InProgressAsync();
        var got = server.Last.Received.Skip(2).ToList();
        Assert.Equal(4, got.Count);
        Assert.Equal(0u, got[0].Corr);
        Assert.Equal(new ulong[] { 7 }, Assert.IsType<Request.Ack>(got[0].Req).Offsets);
        Assert.Equal("c", ((Request.Ack)got[0].Req).Consumer);
        Assert.Equal(new Request.Nack("c", 7, 250), got[1].Req);
        Assert.Equal(new Request.Term("c", 7, "poison"), got[2].Req);
        Assert.Equal(new ulong[] { 7 }, Assert.IsType<Request.InProgress>(got[3].Req).Offsets);
        Assert.All(got.Skip(1), r => Assert.NotEqual(0u, r.Corr));
    }

    [Fact]
    public async Task SendsAcksBeforeAnyLaterRequestCoalescedPerConsumer()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.ReplyMany((corr, new Response.SubscribeOk(1)), (0, new Response.Deliver(1, new[] { Rec(1), Rec(2), Rec(3) })));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        var msgs = new[] { (await sub.NextAsync())!, (await sub.NextAsync())!, (await sub.NextAsync())! };
        foreach (var m in msgs)
        {
            m.Ack();
        }
        _ = c.PingAsync();
        await FakeServer.Until(() => server.Last.Of<Request.Ping>().Count == 1);
        // (window 256: no Credit is due after three messages)
        var types = server.Last.Received.Skip(2).Select(r => r.Req.GetType().Name).ToList();
        Assert.Equal("Ping", types[^1]);
        Assert.All(types.Take(types.Count - 1), t => Assert.Equal("Ack", t));
        var acks = server.Last.Of<Request.Ack>();
        Assert.All(acks, a => Assert.Equal(0u, a.Corr));
        Assert.Equal(new ulong[] { 1, 2, 3 }, acks.SelectMany(a => a.Req.Offsets));
        Assert.True(acks.Count <= 3);
    }

    [Fact]
    public async Task CoalescesAcksMadeTogetherIntoOneFrame()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.ReplyMany((corr, new Response.SubscribeOk(1)), (0, new Response.Deliver(1, Enumerable.Range(0, 200).Select(i => Rec((ulong)i)).ToList())));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c", new SubscribeOptions { Window = 1000 });
        await FakeServer.Until(() => sub.Buffered == 200);
        var msgs = new List<Message>();
        for (int i = 0; i < 200; i++)
        {
            msgs.Add((await sub.NextAsync())!);
        }
        foreach (var m in msgs)
        {
            m.Ack();
        }
        await FakeServer.Until(() => server.Last.Of<Request.Ack>().Sum(a => a.Req.Offsets.Count) == 200);
        // Far fewer frames than acks.
        Assert.True(server.Last.Of<Request.Ack>().Count < 50, $"{server.Last.Of<Request.Ack>().Count} ack frames");
    }

    [Fact]
    public async Task FlushesQueuedAcksOnClose()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.ReplyMany((corr, new Response.SubscribeOk(1)), (0, new Response.Deliver(1, new[] { Rec(4) })));
                return true;
            }
            return false;
        });
        var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        (await sub.NextAsync())!.Ack();
        var conn = server.Last;
        await c.CloseAsync();
        await FakeServer.Until(() => conn.Of<Request.Ack>().Count == 1);
        Assert.Equal(new EndReason(0, "client closed"), sub.EndReason);
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => c.PingAsync());
    }

    [Fact]
    public async Task ReportsFailedFireAndForgetRequestsAsAsyncErrorEvents()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var errors = new List<Exception>();
        c.AsyncError += (_, e) =>
        {
            lock (errors)
            {
                errors.Add(e.Error!);
            }
        };
        server.Last.Reply(0, new Response.Error(404, "consumer 'x' not found", null));
        await FakeServer.Until(() => errors.Count == 1);
        Assert.Equal(404, Assert.IsType<ExspeedServerException>(errors[0]).Code);
    }

    [Fact]
    public async Task EndsOnSubscriptionEndedAfterYieldingBufferedRecords()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.ReplyMany(
                    (corr, new Response.SubscribeOk(3)),
                    (0, new Response.Deliver(3, new[] { Rec(0) })),
                    (0, new Response.SubscriptionEnded(3, 404, "consumer deleted")));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        await FakeServer.Until(() => sub.IsClosed);
        var seen = new List<ulong>();
        await foreach (var m in sub)
        {
            seen.Add(m.Offset);
        }
        Assert.Equal(new ulong[] { 0 }, seen);
        Assert.Equal(new EndReason(404, "consumer deleted"), sub.EndReason);
    }

    [Fact]
    public async Task UnsubscribesWhenAnAwaitForeachLoopBreaks()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Subscribe:
                    conn.ReplyMany((corr, new Response.SubscribeOk(4)), (0, new Response.Deliver(4, new[] { Rec(0), Rec(1) })));
                    return true;
                case Request.Unsubscribe:
                    conn.Reply(corr, new Response.Ok());
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        await foreach (var m in sub)
        {
            break;
        }
        Assert.Equal(new Request.Unsubscribe(4), server.Last.Of<Request.Unsubscribe>()[0].Req);
        Assert.Equal(new EndReason(0, "unsubscribed"), sub.EndReason);
        Assert.Null(await sub.NextAsync());
    }

    [Fact]
    public async Task NextReturnsNullOnTimeoutAndHonoursCancellation()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.Reply(corr, new Response.SubscribeOk(2));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        Assert.Null(await sub.NextAsync(TimeSpan.FromMilliseconds(50)));
        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sub.NextAsync(null, cts.Token));
        // A message delivered after a waiter gave up is not lost.
        server.Last.Reply(0, new Response.Deliver(2, new[] { Rec(9) }));
        Assert.Equal(9UL, (await sub.NextAsync(TimeSpan.FromSeconds(1)))!.Offset);
    }

    [Fact]
    public async Task EndsSubscriptionsWith503WhenTheConnectionIsLostAndReconnectIsOff()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Subscribe)
            {
                conn.Reply(corr, new Response.SubscribeOk(1));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeAsync("c");
        var closed = new TaskCompletionSource<Exception?>();
        c.Closed += (_, e) => closed.TrySetResult(e.Error);
        var next = sub.NextAsync();
        server.Last.Destroy();
        Assert.Null(await next);
        Assert.IsType<ExspeedConnectionException>(await closed.Task.WaitAsync(TimeSpan.FromSeconds(3)));
        Assert.Equal(503, sub.EndReason?.Code);
        Assert.False(c.IsConnected);
    }

    // ---- reconnection ---------------------------------------------------------------

    [Fact]
    public async Task ReconnectsRecreatesEphemeralConsumersAndResubscribes()
    {
        uint nextSub = 1;
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Subscribe:
                    conn.Reply(corr, new Response.SubscribeOk(Interlocked.Increment(ref nextSub) - 1));
                    return true;
                case Request.CreateConsumer:
                    conn.Reply(corr, Response.Json.Of("{}"));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect() });
        await c.CreateConsumerAsync(new ConsumerSpec("tmp", "s") { Ephemeral = true });
        var sub = await c.SubscribeAsync("tmp", new SubscribeOptions { Window = 8 });
        Assert.Equal(1u, sub.Id);
        server.Last.Reply(0, new Response.Deliver(1, new[] { Rec(0) }));
        Assert.Equal(0UL, (await sub.NextAsync())!.Offset);

        var events = new List<string>();
        var reconnected = new TaskCompletionSource();
        c.Disconnected += (_, _) =>
        {
            lock (events)
            {
                events.Add("disconnect");
            }
        };
        c.Reconnected += (_, _) =>
        {
            lock (events)
            {
                events.Add("reconnect");
            }
            reconnected.TrySetResult();
        };
        server.Conns[0].Destroy();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { "disconnect", "reconnect" }, events);
        Assert.Equal(2, server.Conns.Count);
        var second = server.Conns[1];
        Assert.Equal(new[] { "Connect", "CreateConsumer", "Subscribe" }, second.Received.Select(r => r.Req.GetType().Name));
        Assert.Equal(new Request.Subscribe("tmp", 8), second.Of<Request.Subscribe>()[0].Req);
        Assert.Equal(2u, sub.Id);

        second.Reply(0, new Response.Deliver(2, new[] { Rec(1) }));
        Assert.Equal(1UL, (await sub.NextAsync(TimeSpan.FromSeconds(1)))!.Offset);
        Assert.True(c.IsConnected);
    }

    [Fact]
    public async Task EndsASubscriptionWhoseResubscribeFails()
    {
        int subscribes = 0;
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.Subscribe)
            {
                return false;
            }
            if (Interlocked.Increment(ref subscribes) == 1)
            {
                conn.Reply(corr, new Response.SubscribeOk(1));
            }
            else
            {
                conn.Reply(corr, new Response.Error(404, "consumer 'c' not found", null));
            }
            return true;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect() });
        var sub = await c.SubscribeAsync("c");
        var reconnected = new TaskCompletionSource();
        c.Reconnected += (_, _) => reconnected.TrySetResult();
        server.Conns[0].Destroy();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Null(await sub.NextAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(new EndReason(404, "consumer 'c' not found"), sub.EndReason);
    }

    [Fact]
    public async Task FailsRequestsWhileReconnecting()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect() with { InitialDelay = TimeSpan.FromMilliseconds(300) } });
        var disconnected = new TaskCompletionSource();
        c.Disconnected += (_, _) => disconnected.TrySetResult();
        server.Last.Destroy();
        await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var err = await Assert.ThrowsAsync<ExspeedConnectionException>(() => c.PingAsync());
        Assert.Contains("reconnecting", err.Message);
    }

    [Fact]
    public async Task GivesUpAfterMaxAttemptsAndCloses()
    {
        var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect(2) });
        var closed = new TaskCompletionSource<Exception?>();
        c.Closed += (_, e) => closed.TrySetResult(e.Error);
        await server.DisposeAsync();
        var err = await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<ExspeedConnectionException>(err);
        Assert.False(c.IsConnected);
        Assert.Contains("closed", (await Assert.ThrowsAsync<ExspeedConnectionException>(() => c.PingAsync())).Message);
    }

    [Fact]
    public async Task StopsReconnectingWhenTheCredentialIsRejected()
    {
        int connects = 0;
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Connect && Interlocked.Increment(ref connects) > 1)
            {
                conn.Reply(corr, new Response.Error(401, "unauthorized", null));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect() });
        var closed = new TaskCompletionSource<Exception?>();
        c.Closed += (_, e) => closed.TrySetResult(e.Error);
        server.Last.Destroy();
        var err = await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(401, Assert.IsType<ExspeedServerException>(err).Code);
        Assert.Equal(2, connects);
    }

    // ---- keepalive -------------------------------------------------------------------

    [Fact]
    public async Task PingsOnTheConfiguredInterval()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Keepalive = TimeSpan.FromMilliseconds(30) });
        await FakeServer.Until(() => server.Last.Of<Request.Ping>().Count >= 2);
    }

    [Fact]
    public async Task DropsTheConnectionWhenAKeepalivePingTimesOut()
    {
        await using var server = FakeServer.Start((_, _, req) => req is Request.Ping); // never answers pings
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with
        {
            Keepalive = TimeSpan.FromMilliseconds(30),
            RequestTimeout = TimeSpan.FromMilliseconds(100),
        });
        var closed = new TaskCompletionSource<Exception?>();
        c.Closed += (_, e) => closed.TrySetResult(e.Error);
        var err = await closed.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Contains("keepalive", err!.Message);
    }

    // ---- cluster leader discovery ------------------------------------------------------

    [Fact]
    public async Task ConnectsToTheLeaderByFollowingHintsFromASeed()
    {
        int leaderPort = 0;
        static Response Meta(bool isLeader, string? leader) =>
            Response.Json.Of($"{{\"node_id\":\"x\",\"is_leader\":{(isLeader ? "true" : "false")},\"leader\":{(leader is null ? "null" : $"\"{leader}\"")},\"server_version\":\"t\"}}");
        await using var follower = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Connect:
                    conn.Reply(corr, new Response.ConnectOk("t", "f", $"127.0.0.1:{leaderPort}"));
                    return true;
                case Request.Metadata:
                    conn.Reply(corr, Meta(false, $"127.0.0.1:{leaderPort}"));
                    return true;
                default:
                    return false;
            }
        });
        await using var leader = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.Connect:
                    conn.Reply(corr, new Response.ConnectOk("t", "l", null));
                    return true;
                case Request.Metadata:
                    conn.Reply(corr, Meta(true, null));
                    return true;
                default:
                    return false;
            }
        });
        leaderPort = leader.Port;
        await using var c = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
        {
            Servers = new[] { $"127.0.0.1:{follower.Port}" },
            Keepalive = TimeSpan.Zero,
            Reconnect = FastReconnect(20),
        });
        Assert.Equal("l", c.ServerInfo.NodeId);

        // Without seeds, a handshake naming another leader is followed too.
        await using var c2 = await ExspeedClient.ConnectAsync(Opts(follower));
        Assert.Equal("l", c2.ServerInfo.NodeId);
    }
}
