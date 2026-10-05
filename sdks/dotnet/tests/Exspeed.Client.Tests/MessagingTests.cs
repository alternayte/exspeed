using System.Text;
using Exspeed.Protocol;
using static Exspeed.Tests.ClientTests;

namespace Exspeed.Tests;

/// <summary>Core pub/sub, request-reply, KV buckets and consumer info against the fake server.</summary>
public class MessagingTests
{
    private static void Ok(FakeConn conn, uint corr) => conn.Reply(corr, new Response.Ok());

    /// <summary>A KV record as the server stores it: subject = key, raw stream offset.</summary>
    private static WireRecord KvRec(ulong offset, string key, string value, string? op = null) => new(
        offset,
        1_700_000_000_000_000_000UL + offset,
        0,
        key,
        null,
        Wire.B(value),
        op is null ? Wire.NoHeaders : new[] { Wire.H("exspeed-kv-op", op) });

    private static Response.CoreMsg Msg(uint subId, string subject, string value, string? replyTo = null) =>
        new(subId, subject, replyTo, Wire.NoHeaders, Wire.B(value));

    // ---- core pub/sub -------------------------------------------------------------

    [Fact]
    public async Task PublishesACoreMessageAndWaitsForOk()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.CorePublish)
            {
                Ok(conn, corr);
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        await c.PublishCoreAsync("orders.created", "{\"id\":1}", new CorePublishOptions { Headers = new[] { Wire.H("trace-id", "t") } });
        var (corr, p) = server.Last.Of<Request.CorePublish>()[0];
        Assert.NotEqual(0u, corr);
        Assert.Equal("orders.created", p.Subject);
        Assert.Null(p.ReplyTo);
        Assert.Equal(new[] { Wire.H("trace-id", "t") }, p.Headers);
        Assert.Equal("{\"id\":1}", Encoding.UTF8.GetString(p.Value.Span));
    }

    [Fact]
    public async Task SubscribesWithAQueueGroupRespondsAndUnsubscribes()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.CoreSubscribe:
                    conn.ReplyMany(
                        (corr, new Response.SubscribeOk(0x80000001)),
                        (0, new Response.CoreMsg(0x80000001, "svc.echo", "_INBOX.x.1", new[] { Wire.H("h", "1") }, Wire.B("{\"q\":1}"))),
                        (0, Msg(0x80000099, "other", "ignored")));
                    return true;
                case Request.CorePublish or Request.Unsubscribe:
                    Ok(conn, corr);
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeCoreAsync("svc.*", new CoreSubscribeOptions { Queue = "workers" });
        Assert.Equal(new Request.CoreSubscribe("svc.*", "workers"), server.Last.Of<Request.CoreSubscribe>()[0].Req);
        Assert.Equal(0x80000001u, sub.Id);
        Assert.Equal(("svc.*", "workers"), (sub.Subject, sub.Queue));
        var m = (await sub.NextAsync(TimeSpan.FromSeconds(1)))!;
        Assert.Equal(("svc.echo", "_INBOX.x.1", "1"), (m.Subject, m.ReplyTo, m.Header("h")));
        Assert.Equal(1, m.Json<Dictionary<string, int>>()!["q"]);
        await m.RespondAsync("pong");
        var resp = server.Last.Of<Request.CorePublish>()[0].Req;
        Assert.Equal(("_INBOX.x.1", (string?)null, "pong"), (resp.Subject, resp.ReplyTo, Encoding.UTF8.GetString(resp.Value.Span)));
        Assert.Null(await sub.NextAsync(TimeSpan.FromMilliseconds(100))); // the other sub's message isn't routed here

        await sub.UnsubscribeAsync();
        Assert.Equal(new Request.Unsubscribe(0x80000001), server.Last.Of<Request.Unsubscribe>()[0].Req);
        Assert.Equal(new EndReason(0, "unsubscribed"), sub.EndReason);
    }

    [Fact]
    public async Task RefusesToRespondToAMessageWithoutReplyTo()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.CoreSubscribe)
            {
                conn.ReplyMany((corr, new Response.SubscribeOk(0x80000001)), (0, Msg(0x80000001, "a", "x")));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeCoreAsync("a");
        var m = (await sub.NextAsync())!;
        await Assert.ThrowsAsync<ExspeedException>(() => m.RespondAsync("no"));
    }

    [Fact]
    public async Task EndsWhenTheServerEndsItAfterYieldingBufferedMessages()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.CoreSubscribe)
            {
                conn.ReplyMany(
                    (corr, new Response.SubscribeOk(0x80000002)),
                    (0, Msg(0x80000002, "a", "1")),
                    (0, new Response.SubscriptionEnded(0x80000002, 503, "leadership moved")));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeCoreAsync("a");
        await FakeServer.Until(() => sub.IsClosed);
        var seen = new List<string>();
        await foreach (var m in sub)
        {
            seen.Add(m.Text());
        }
        Assert.Equal(new[] { "1" }, seen);
        Assert.Equal(new EndReason(503, "leadership moved"), sub.EndReason);
    }

    [Fact]
    public async Task UnsubscribesWhenAnAwaitForeachLoopBreaks()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.CoreSubscribe:
                    conn.ReplyMany((corr, new Response.SubscribeOk(0x80000003)), (0, Msg(0x80000003, "a", "1")));
                    return true;
                case Request.Unsubscribe:
                    Ok(conn, corr);
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var sub = await c.SubscribeCoreAsync("a");
        await foreach (var m in sub)
        {
            break;
        }
        Assert.Equal(new Request.Unsubscribe(0x80000003), server.Last.Of<Request.Unsubscribe>()[0].Req);
        Assert.True(sub.IsClosed);
    }

    // ---- request-reply ----------------------------------------------------------------

    /// <summary>Answers CoreSubscribe for the inbox and records requests; replies are sent by the test.</summary>
    private static List<string> InboxServer(FakeServer server, uint firstId = 0x80000010)
    {
        var subs = new List<string>();
        uint id = firstId;
        server.Handler = (conn, corr, req) =>
        {
            switch (req)
            {
                case Request.CoreSubscribe cs:
                    lock (subs)
                    {
                        subs.Add(cs.Subject);
                    }
                    conn.Reply(corr, new Response.SubscribeOk(id++));
                    return true;
                case Request.CorePublish:
                    Ok(conn, corr);
                    return true;
                default:
                    return false;
            }
        };
        return subs;
    }

    [Fact]
    public async Task SharesOneInboxAndRoutesResponsesByTheLastSubjectToken()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var subs = InboxServer(server);
        var a = c.RequestAsync("svc.a", "1");
        var b = c.RequestAsync("svc.b", "{\"n\":2}", new CoreRequestOptions { Headers = new[] { Wire.H("h", "v") } });
        await FakeServer.Until(() => server.Last.Of<Request.CorePublish>().Count == 2);
        Assert.Single(subs);
        Assert.Matches("^_INBOX\\.[0-9a-f]{24}\\.\\*$", subs[0]);
        var prefix = subs[0][..^2];
        var pubs = server.Last.Of<Request.CorePublish>().OrderBy(p => p.Req.Subject).ToList();
        var (pa, pb) = (pubs[0].Req, pubs[1].Req);
        Assert.StartsWith(prefix + ".", pa.ReplyTo);
        Assert.StartsWith(prefix + ".", pb.ReplyTo);
        Assert.NotEqual(pa.ReplyTo, pb.ReplyTo);
        Assert.Equal(new[] { Wire.H("h", "v") }, pb.Headers);
        // Out of order, on the inbox subscription.
        server.Last.Reply(0, Msg(0x80000010, pb.ReplyTo!, "B"));
        server.Last.Reply(0, Msg(0x80000010, pa.ReplyTo!, "A"));
        Assert.Equal("A", (await a).Text());
        Assert.Equal("B", (await b).Text());

        var third = c.RequestAsync("svc.a", "3");
        await FakeServer.Until(() => server.Last.Of<Request.CorePublish>().Count == 3);
        Assert.Single(subs); // still the same inbox
        var p3 = server.Last.Of<Request.CorePublish>()[2].Req;
        server.Last.Reply(0, Msg(0x80000010, p3.ReplyTo!, "C"));
        Assert.Equal("C", (await third).Text());
    }

    [Fact]
    public async Task FailsAtOnceWith404WhenThereAreNoResponders()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        InboxServer(server);
        var inner = server.Handler;
        server.Handler = (conn, corr, req) =>
        {
            if (req is Request.CorePublish)
            {
                conn.Reply(corr, new Response.Error(404, "no responders for 'svc.none'", null));
                return true;
            }
            return inner(conn, corr, req);
        };
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => c.RequestAsync("svc.none", "x", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) }));
        Assert.Equal(404, err.Code);
        Assert.True(sw.ElapsedMilliseconds < 1000);
    }

    [Fact]
    public async Task TimesOutAndIgnoresALateResponse()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        InboxServer(server);
        await Assert.ThrowsAsync<ExspeedTimeoutException>(() => c.RequestAsync("svc.slow", "x", new CoreRequestOptions { Timeout = TimeSpan.FromMilliseconds(100) }));
        var p = server.Last.Of<Request.CorePublish>()[0].Req;
        server.Last.Reply(0, Msg(0x80000010, p.ReplyTo!, "late"));
        await c.PingAsync(); // the late response was dropped without trouble
    }

    [Fact]
    public async Task FailsWaitingRequestsWhenTheInboxEndsAndSubscribesANewOneNextTime()
    {
        await using var server = FakeServer.Start();
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var subs = InboxServer(server);
        var pending = c.RequestAsync("svc.a", "x", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) });
        await FakeServer.Until(() => server.Last.Of<Request.CorePublish>().Count == 1);
        server.Last.Reply(0, new Response.SubscriptionEnded(0x80000010, 503, "leadership moved"));
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => pending);
        Assert.Equal(503, err.Code);

        var again = c.RequestAsync("svc.a", "y");
        await FakeServer.Until(() => server.Last.Of<Request.CorePublish>().Count == 2);
        Assert.Equal(2, subs.Count);
        Assert.NotEqual(subs[0], subs[1]);
        var p = server.Last.Of<Request.CorePublish>()[1].Req;
        Assert.StartsWith(subs[1][..^2], p.ReplyTo);
        server.Last.Reply(0, Msg(0x80000011, p.ReplyTo!, "ok"));
        Assert.Equal("ok", (await again).Text());
    }

    [Fact]
    public async Task AfterAReconnectResubscribesCoreSubscriptionsAndSetsUpANewInbox()
    {
        await using var server = FakeServer.Start();
        uint id = 0x80000020;
        server.Handler = (conn, corr, req) =>
        {
            switch (req)
            {
                case Request.CoreSubscribe:
                    conn.Reply(corr, new Response.SubscribeOk(Interlocked.Increment(ref id) - 1));
                    return true;
                case Request.CorePublish:
                    Ok(conn, corr);
                    return true;
                default:
                    return false;
            }
        };
        await using var c = await ExspeedClient.ConnectAsync(Opts(server) with { Reconnect = FastReconnect() });
        var sub = await c.SubscribeCoreAsync("events.>", new CoreSubscribeOptions { Queue = "g" });
        var pending = c.RequestAsync("svc.a", "x", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) });
        await FakeServer.Until(() => server.Last.Of<Request.CorePublish>().Count == 1);
        var firstInbox = server.Last.Of<Request.CoreSubscribe>()[1].Req.Subject;

        var reconnected = new TaskCompletionSource();
        c.Reconnected += (_, _) => reconnected.TrySetResult();
        server.Conns[0].Destroy();
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => pending);
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = server.Conns[1];
        Assert.Equal(new[] { new Request.CoreSubscribe("events.>", "g") }, second.Of<Request.CoreSubscribe>().Select(r => r.Req));
        second.Reply(0, Msg(sub.Id, "events.x", "after"));
        Assert.Equal("after", (await sub.NextAsync(TimeSpan.FromSeconds(1)))!.Text());

        var again = c.RequestAsync("svc.a", "y");
        await FakeServer.Until(() => second.Of<Request.CorePublish>().Count == 1);
        var inbox = second.Of<Request.CoreSubscribe>()[1];
        Assert.NotEqual(firstInbox, inbox.Req.Subject);
        var p = second.Of<Request.CorePublish>()[0].Req;
        second.Reply(0, Msg(id - 1, p.ReplyTo!, "ok"));
        Assert.Equal("ok", (await again).Text());
    }

    // ---- kv buckets -----------------------------------------------------------------

    [Fact]
    public async Task EncodesBucketWritesAndTakesRevisionsFromPublishOk()
    {
        ulong rev = 0;
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.KvCreateBucket or Request.DeleteStream:
                    Ok(conn, corr);
                    return true;
                case Request.KvPut or Request.KvDelete:
                    conn.Reply(corr, new Response.PublishOk(Interlocked.Increment(ref rev), false));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var kv = c.Kv("cfg");
        Assert.Equal("KV_cfg", kv.Stream);
        await kv.CreateAsync(new KvBucketOptions { History = 5, Ttl = TimeSpan.FromMinutes(1) });
        await c.Kv("plain").CreateAsync();
        Assert.Equal(1UL, await kv.PutAsync("a", "{\"on\":true}", new KvPutOptions { Ttl = TimeSpan.FromMilliseconds(500) }));
        Assert.Equal(2UL, await kv.CreateKeyAsync("b", "x"));
        Assert.Equal(3UL, await kv.UpdateAsync("b", "y", 2));
        Assert.Equal(4UL, await kv.DeleteAsync("a"));
        Assert.Equal(5UL, await kv.PurgeAsync("b", new KvDeleteOptions { ExpectedRevision = 3 }));
        Assert.Equal(6UL, await kv.PutAsync("c", new byte[] { 1 }));
        await kv.DestroyAsync();

        var reqs = server.Last.Received.Select(r => r.Req).Where(r => r is not Request.Connect).ToList();
        Assert.Equal(new Request.KvCreateBucket("cfg", 5, 60_000, 0), reqs[0]);
        Assert.Equal(new Request.KvCreateBucket("plain", 0, 0, 0), reqs[1]);
        static string Put(Request r)
        {
            var p = (Request.KvPut)r;
            return $"{p.Bucket}/{p.Key}={Encoding.UTF8.GetString(p.Value.Span)} exp={p.ExpectedRevision?.ToString() ?? "-"} ttl={p.TtlMs?.ToString() ?? "-"}";
        }
        Assert.Equal("cfg/a={\"on\":true} exp=- ttl=500", Put(reqs[2]));
        Assert.Equal("cfg/b=x exp=0 ttl=-", Put(reqs[3]));
        Assert.Equal("cfg/b=y exp=2 ttl=-", Put(reqs[4]));
        Assert.Equal(new Request.KvDelete("cfg", "a", false, null), reqs[5]);
        Assert.Equal(new Request.KvDelete("cfg", "b", true, 3), reqs[6]);
        Assert.Equal(new Request.DeleteStream("KV_cfg"), reqs[8]);
    }

    [Fact]
    public async Task PassesACasConflictThroughAsServerException409()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.KvPut)
            {
                conn.Reply(corr, new Response.Error(409, "wrong revision", Wire.B("{\"current_revision\":7}")));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => c.Kv("b").UpdateAsync("k", "v", 3));
        Assert.Equal(409, err.Code);
        Assert.Equal(7, err.Detail!.Value.GetProperty("current_revision").GetInt32());
    }

    [Fact]
    public async Task TurnsRecordsIntoEntriesWithRevisionOffsetPlusOneAndKeyNotFoundIntoNull()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.KvGet g)
            {
                return false;
            }
            switch (g.Key)
            {
                case "live":
                    conn.Reply(corr, new Response.Messages(new[] { KvRec(4, "live", "{\"n\":1}") }));
                    break;
                case "gone":
                    conn.Reply(corr, new Response.Error(404, "key 'gone' not found", null));
                    break;
                case "old":
                    conn.Reply(corr, new Response.Messages(new[] { KvRec(g.Revision!.Value - 1, "old", "v1") }));
                    break;
                default:
                    conn.Reply(corr, new Response.Messages(Array.Empty<WireRecord>()));
                    break;
            }
            return true;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var kv = c.Kv("b");
        var e = (await kv.GetAsync("live"))!;
        Assert.Equal(("live", 5UL, KvOp.Put), (e.Key, e.Revision, e.Op));
        Assert.Equal(1, e.Json<Dictionary<string, int>>()!["n"]);
        Assert.Equal(1_700_000_000_000_000_004UL, e.TimestampNs);
        Assert.Equal(1_700_000_000_000, e.Timestamp.ToUnixTimeMilliseconds());
        Assert.Null(await kv.GetAsync("gone"));
        Assert.Null(await kv.GetAsync("empty"));
        var old = (await kv.GetRevisionAsync("old", 2))!;
        Assert.Equal((2UL, "v1"), (old.Revision, old.Text()));
        Assert.Equal(new ulong?[] { null, null, null, 2 }, server.Last.Of<Request.KvGet>().Select(r => r.Req.Revision));
    }

    [Fact]
    public async Task ThrowsWhenTheBucketDoesNotExist()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.KvGet)
            {
                conn.Reply(corr, new Response.Error(404, "bucket 'nope' not found", null));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => c.Kv("nope").GetAsync("k"));
        Assert.Equal(404, err.Code);
    }

    [Fact]
    public async Task ListsKeysAndHistoryWithTombstones()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.KvKeys:
                    conn.Reply(corr, Response.Json.Of("[\"a.1\",\"a.2\"]"));
                    return true;
                case Request.KvHistory:
                    conn.Reply(corr, new Response.Messages(new[] { KvRec(0, "k", "v1"), KvRec(3, "k", "", "DEL"), KvRec(5, "k", "v2"), KvRec(6, "k", "", "PURGE") }));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var kv = c.Kv("b");
        Assert.Equal(new[] { "a.1", "a.2" }, await kv.KeysAsync("a.*"));
        Assert.Equal(new[] { "a.1", "a.2" }, await kv.KeysAsync());
        Assert.Equal(new[] { "a.*", "" }, server.Last.Of<Request.KvKeys>().Select(r => r.Req.Filter));
        var h = await kv.HistoryAsync("k");
        Assert.Equal(
            new[] { (1UL, KvOp.Put), (4UL, KvOp.Delete), (6UL, KvOp.Put), (7UL, KvOp.Purge) },
            h.Select(e => (e.Revision, e.Op)));
    }

    [Fact]
    public async Task WatchesLiveKeysFirstSortedByRevisionThenEveryChange()
    {
        var reads = new List<Request.Read>();
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.Read r)
            {
                return false;
            }
            lock (reads)
            {
                reads.Add(r);
            }
            if (r.WaitMs == 0 && r.From == 0)
            {
                // Snapshot, page 1 of 2 (high watermark 5).
                conn.Reply(corr, new Response.ReadResult(3, 5, new[] { KvRec(0, "a", "a1"), KvRec(1, "b", "b1"), KvRec(2, "a", "a2") }));
            }
            else if (r.WaitMs == 0 && r.From == 3)
            {
                conn.Reply(corr, new Response.ReadResult(5, 6, new[] { KvRec(3, "c", "c1"), KvRec(4, "b", "", "DEL") }));
            }
            else
            {
                conn.Reply(corr, new Response.ReadResult(7, 7, new[] { KvRec(5, "a", "a3"), KvRec(6, "c", "", "DEL") }));
            }
            return true;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var w = c.Kv("b").Watch("x.>");
        var seen = new List<(string, ulong, KvOp)>();
        await foreach (var e in w)
        {
            seen.Add((e.Key, e.Revision, e.Op));
            if (seen.Count == 4)
            {
                break;
            }
        }
        // b was deleted before the snapshot ended: left out. a (rev 3) before c (rev 4).
        Assert.Equal(new[] { ("a", 3UL, KvOp.Put), ("c", 4UL, KvOp.Put), ("a", 6UL, KvOp.Put), ("c", 7UL, KvOp.Delete) }, seen);
        Assert.Equal(
            new[] { ("KV_b", 0UL, 0u, "x.>"), ("KV_b", 3UL, 0u, "x.>"), ("KV_b", 5UL, 10_000u, "x.>") },
            reads.Take(3).Select(r => (r.Stream, r.From, r.WaitMs, r.Filter)));
        Assert.True(w.IsClosed);
        Assert.Null(await w.NextAsync());
    }

    [Fact]
    public async Task WatchNextReturnsNullOnTimeoutWhileALongPollIsPendingKeepingWhatItBrings()
    {
        (FakeConn Conn, uint Corr)? held = null;
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is not Request.Read r)
            {
                return false;
            }
            if (r.WaitMs == 0)
            {
                conn.Reply(corr, new Response.ReadResult(0, 0, Array.Empty<WireRecord>()));
            }
            else
            {
                held = (conn, corr);
            }
            return true;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var w = c.Kv("b").Watch();
        Assert.Null(await w.NextAsync(TimeSpan.FromMilliseconds(100)));
        await FakeServer.Until(() => held is not null);
        held!.Value.Conn.Reply(held.Value.Corr, new Response.ReadResult(1, 1, new[] { KvRec(0, "k", "v") }));
        var e = (await w.NextAsync(TimeSpan.FromSeconds(1)))!;
        Assert.Equal(("k", 1UL), (e.Key, e.Revision));
        Assert.Equal(2, server.Last.Of<Request.Read>().Count); // the timed-out NextAsync didn't start another read
        w.Stop();
    }

    [Fact]
    public async Task WatchSurfacesAFailedRead()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Read)
            {
                conn.Reply(corr, new Response.Error(404, "stream 'KV_b' not found", null));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var w = c.Kv("b").Watch();
        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => w.NextAsync(TimeSpan.FromSeconds(2)))).Code);
    }

    // ---- consumer info ---------------------------------------------------------------

    [Fact]
    public async Task ParsesConsumerInfoKeepingFilterHeaderKeysAndNumDelayed()
    {
        const string info =
            "{\"spec\":{\"name\":\"c\",\"stream\":\"s\",\"filter_subjects\":[\"a.>\"],\"deliver\":{\"from_offset\":3},\"ack\":\"none\","
            + "\"ack_wait_ms\":1000,\"max_deliver\":2,\"backoff_ms\":[10,20],\"max_ack_pending\":7,\"dlq_stream\":\"d\",\"ephemeral\":true,"
            + "\"filter_headers\":{\"x_tenant_id\":\"acme\"},\"header_match\":\"any\",\"single_active\":true,\"priority_window\":5,\"dead_letter_expired\":true},"
            + "\"next_offset\":10,\"ack_floor\":4,\"num_unacked\":3,\"num_in_flight\":2,\"num_delayed\":2,\"num_waiting\":6,\"lag\":6,"
            + "\"subscribers\":1,\"pull_waiters\":0,\"stats\":{\"delivered\":9,\"redelivered\":1,\"acked\":5,\"dead_lettered\":1,\"gone\":0,\"skipped\":0}}";
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            switch (req)
            {
                case Request.ConsumerInfo or Request.CreateConsumer:
                    conn.Reply(corr, Response.Json.Of(info));
                    return true;
                case Request.ListConsumers:
                    conn.Reply(corr, Response.Json.Of($"[{info},{{\"spec\":{{\"name\":\"d\",\"stream\":\"s\"}},\"stats\":{{}}}}]"));
                    return true;
                default:
                    return false;
            }
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var i = await c.ConsumerInfoAsync("c");
        Assert.Equal(("c", "s"), (i.Spec.Name, i.Spec.Stream));
        Assert.Equal(new[] { "a.>" }, i.Spec.FilterSubjects);
        Assert.Equal(DeliverPolicy.FromOffset(3), i.Spec.Deliver);
        Assert.Equal(AckPolicy.None, i.Spec.Ack);
        Assert.Equal(TimeSpan.FromSeconds(1), i.Spec.AckWait);
        Assert.Equal(2u, i.Spec.MaxDeliver);
        Assert.Equal(new[] { TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(20) }, i.Spec.Backoff);
        Assert.Equal(7u, i.Spec.MaxAckPending);
        Assert.Equal("d", i.Spec.DlqStream);
        Assert.True(i.Spec.Ephemeral);
        Assert.Equal("acme", i.Spec.FilterHeaders!["x_tenant_id"]);
        Assert.Equal(HeaderMatch.Any, i.Spec.HeaderMatch);
        Assert.True(i.Spec.SingleActive);
        Assert.Equal(5u, i.Spec.PriorityWindow);
        Assert.True(i.Spec.DeadLetterExpired);
        Assert.Equal((10UL, 4UL, 3UL, 2UL, 2UL, 6UL, 6UL, 1UL, 0UL), (i.NextOffset, i.AckFloor, i.NumUnacked, i.NumInFlight, i.NumDelayed, i.NumWaiting, i.Lag, i.Subscribers, i.PullWaiters));
        Assert.Equal(new ConsumerStats { Delivered = 9, Redelivered = 1, Acked = 5, DeadLettered = 1 }, i.Stats);

        var created = await c.CreateConsumerAsync(new ConsumerSpec("c", "s")
        {
            FilterHeaders = new Dictionary<string, string> { ["x_tenant_id"] = "acme" },
            HeaderMatch = HeaderMatch.Any,
        });
        Assert.Equal("acme", created.Spec.FilterHeaders!["x_tenant_id"]);
        var sent = Encoding.UTF8.GetString(server.Last.Of<Request.CreateConsumer>()[0].Req.SpecJson.Span);
        Assert.Equal("{\"name\":\"c\",\"stream\":\"s\",\"filter_headers\":{\"x_tenant_id\":\"acme\"},\"header_match\":\"any\"}", sent);

        var list = await c.ListConsumersAsync();
        Assert.Equal(2, list.Count);
        var d = list[1].Spec;
        // Defaults filled in for a minimal spec.
        Assert.Equal(DeliverPolicy.All, d.Deliver);
        Assert.Equal(AckPolicy.Explicit, d.Ack);
        Assert.Equal(TimeSpan.FromSeconds(30), d.AckWait);
        Assert.Equal(5u, d.MaxDeliver);
        Assert.Equal(1000u, d.MaxAckPending);
        Assert.Empty(d.FilterHeaders!);
        Assert.Null(d.DlqStream);
    }
}
