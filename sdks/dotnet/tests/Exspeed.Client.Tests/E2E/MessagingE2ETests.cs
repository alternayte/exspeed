using System.Diagnostics;
using static Exspeed.Tests.E2E.TestServer;

namespace Exspeed.Tests.E2E;

/// <summary>Stream limits, time headers, routing settings, core messaging, request-reply and KV against a real server.</summary>
[Collection("e2e")]
public sealed class MessagingE2ETests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _f;

    public MessagingE2ETests(ServerFixture f)
    {
        _f = f;
    }

    private ExspeedClient Client => _f.Client;

    private static KeyValuePair<string, string> H(string k, string v) => new(k, v);

    // ---- stream limits and time headers ----------------------------------------------

    [E2EFact]
    public async Task HidesRecordsWhoseTtlHasPassedFromReadsAndConsumers()
    {
        var s = Uniq("ttl");
        await Client.CreateStreamAsync(new StreamSpec(s) { AllowMsgTtl = true });
        Assert.True((await Client.StreamInfoAsync(s)).Config.AllowMsgTtl);

        await Client.PublishAsync(s, new PublishRecord("jobs.a", "short") { Ttl = TimeSpan.FromMilliseconds(150) });
        await Client.PublishAsync(s, new PublishRecord("jobs.a", "keep"));
        await Client.PublishAsync(s, new PublishRecord("jobs.a", "long") { Ttl = TimeSpan.FromHours(1) });
        var c = Uniq("ttl-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await Task.Delay(400);

        Assert.Equal(new[] { "keep", "long" }, (await Client.ReadAsync(s)).Records.Select(r => r.Text()));
        var got = await Client.PullAsync(c, new PullOptions { MaxMessages = 10, Expires = TimeSpan.FromMilliseconds(500) });
        Assert.Equal(new[] { "keep", "long" }, got.Select(m => m.Text()));
        Assert.Equal("3600000ms", got[1].Header("exspeed-ttl"));
    }

    [E2EFact]
    public async Task RejectsTimeHeadersOnAStreamThatDoesNotAllowThem()
    {
        var s = Uniq("plain");
        await Client.CreateStreamAsync(s);
        foreach (var r in new[]
        {
            new PublishRecord("a.b", "x") { Ttl = TimeSpan.FromSeconds(1) },
            new PublishRecord("a.b", "x") { Delay = TimeSpan.FromSeconds(1) },
        })
        {
            Assert.Equal(400, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.PublishAsync(s, r))).Code);
        }
    }

    [E2EFact]
    public async Task DeliversDelayedRecordsToConsumersWhenDue()
    {
        var s = Uniq("delay");
        await Client.CreateStreamAsync(new StreamSpec(s) { AllowDelayed = true });
        var c = Uniq("delay-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await Client.PublishAsync(s, new PublishRecord("later.a", "delayed") { Delay = TimeSpan.FromMilliseconds(700) });
        await Client.PublishAsync(s, new PublishRecord("later.a", "at") { DeliverAt = DateTimeOffset.UtcNow.AddMilliseconds(900) });
        await Client.PublishAsync(s, new PublishRecord("later.a", "now"));

        var first = await Client.PullAsync(c, new PullOptions { MaxMessages = 10, Expires = TimeSpan.FromMilliseconds(300) });
        Assert.Equal(new[] { "now" }, first.Select(m => m.Text()));
        await Client.AckAsync(c, first.Select(m => m.Offset));
        Assert.Equal(2UL, (await Client.ConsumerInfoAsync(c)).NumDelayed);

        var due = new List<string>();
        await Eventually(async () =>
        {
            foreach (var m in await Client.PullAsync(c, new PullOptions { MaxMessages = 10, Expires = TimeSpan.FromMilliseconds(200) }))
            {
                due.Add(m.Text());
                m.Ack();
            }
            return due.Count == 2;
        });
        Assert.Equal(new[] { "delayed", "at" }, due);
        // A stateless read sees every record right away: delays apply to consumers.
        Assert.Equal(new[] { "delayed", "at", "now" }, (await Client.ReadAsync(s)).Records.Select(r => r.Text()));
    }

    [E2EFact]
    public async Task KeepsAtMostMaxMsgsDroppingTheOldestOrRejectingNewOnes()
    {
        var old = Uniq("max-old");
        await Client.CreateStreamAsync(new StreamSpec(old) { MaxMsgs = 2 });
        foreach (var v in new[] { "1", "2", "3" })
        {
            await Client.PublishAsync(old, "m.a", v);
        }
        Assert.Equal(new[] { "2", "3" }, (await Client.ReadAsync(old)).Records.Select(r => r.Text()));

        var strict = Uniq("max-new");
        await Client.CreateStreamAsync(new StreamSpec(strict) { MaxMsgs = 2, Discard = DiscardPolicy.New });
        await Client.PublishAsync(strict, "m.a", "1");
        await Client.PublishAsync(strict, "m.a", "2");
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.PublishAsync(strict, "m.a", "3"));
        Assert.Equal(429, err.Code);
        Assert.Equal(new[] { "1", "2" }, (await Client.ReadAsync(strict)).Records.Select(r => r.Text()));
        var cfg = (await Client.StreamInfoAsync(strict)).Config;
        Assert.Equal((2UL, DiscardPolicy.New), (cfg.MaxMsgs, cfg.Discard));
    }

    [E2EFact]
    public async Task KeepsTheNewestRecordsPerSubject()
    {
        var s = Uniq("per-subject");
        await Client.CreateStreamAsync(new StreamSpec(s) { MaxMsgsPerSubject = 1 });
        await Client.PublishAsync(s, "k.a", "a1");
        await Client.PublishAsync(s, "k.b", "b1");
        await Client.PublishAsync(s, "k.a", "a2");
        Assert.Equal(new[] { "b1", "a2" }, (await Client.ReadAsync(s)).Records.Select(r => r.Text()));
    }

    [E2EFact]
    public async Task RemovesAckedRecordsFromAWorkQueueStream()
    {
        var s = Uniq("wq");
        await Client.CreateStreamAsync(new StreamSpec(s) { Retention = RetentionPolicy.WorkQueue });
        Assert.Equal(RetentionPolicy.WorkQueue, (await Client.StreamInfoAsync(s)).Config.Retention);
        var c = Uniq("wq-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await Client.PublishAsync(s, "jobs.x", "1");
        await Client.PublishAsync(s, "jobs.x", "2");
        var msgs = await Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromMilliseconds(500) });
        await Client.AckAsync(c, msgs.Select(m => m.Offset));
        await Eventually(async () => (await Client.ReadAsync(s)).Records.Count == 0);
    }

    [E2EFact]
    public async Task CapturesCoreMessagesPublishedToMatchingSubjects()
    {
        var s = Uniq("capture");
        await Client.CreateStreamAsync(new StreamSpec(s) { CaptureSubjects = new[] { "cap.>" } });
        Assert.Equal(new[] { "cap.>" }, (await Client.StreamInfoAsync(s)).Config.CaptureSubjects);
        await Client.PublishCoreAsync("cap.x", "captured", new CorePublishOptions { Headers = new[] { H("h", "1") } });
        await Client.PublishCoreAsync("other.y", "not captured");
        var records = await Eventually(async () =>
        {
            var r = await Client.ReadAsync(s);
            return r.Records.Count > 0 ? r.Records : null;
        });
        var rec = Assert.Single(records);
        Assert.Equal(("cap.x", "captured"), (rec.Subject, rec.Text()));
        Assert.Equal("1", rec.Header("h"));
        await Client.DeleteStreamAsync(s); // releases the capture filter for other runs
    }

    // ---- consumer routing ---------------------------------------------------------------

    [E2EFact]
    public async Task FiltersByHeadersAllOrAny()
    {
        var s = Uniq("hdr");
        await Client.CreateStreamAsync(s);
        foreach (var (region, tier, v) in new[] { ("eu", "gold", "a"), ("us", "gold", "b"), ("eu", "free", "c"), ("asia", "free", "d") })
        {
            await Client.PublishAsync(s, new PublishRecord("e.x", v) { Headers = new[] { H("region", region), H("tier", tier) } });
        }
        var filter = new Dictionary<string, string> { ["region"] = "eu", ["tier"] = "gold" };
        var all = Uniq("hdr-all");
        var info = await Client.CreateConsumerAsync(new ConsumerSpec(all, s) { FilterHeaders = filter });
        Assert.Equal(filter, info.Spec.FilterHeaders);
        Assert.Equal(new[] { "a" }, (await Client.PullAsync(all, new PullOptions { Expires = TimeSpan.FromMilliseconds(300) })).Select(m => m.Text()));

        var any = Uniq("hdr-any");
        await Client.CreateConsumerAsync(new ConsumerSpec(any, s) { FilterHeaders = filter, HeaderMatch = HeaderMatch.Any });
        Assert.Equal(new[] { "a", "b", "c" }, (await Client.PullAsync(any, new PullOptions { Expires = TimeSpan.FromMilliseconds(300) })).Select(m => m.Text()));
    }

    [E2EFact]
    public async Task DeliversHigherPrioritiesFirstWithinThePriorityWindow()
    {
        var s = Uniq("prio");
        await Client.CreateStreamAsync(s);
        foreach (var (v, p) in new[] { ("low1", 0), ("high1", 9), ("mid", 5), ("low2", 0), ("high2", 9) })
        {
            await Client.PublishAsync(s, new PublishRecord("t.x", v) { Priority = p });
        }
        var c = Uniq("prio-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { PriorityWindow = 100 });
        var order = new List<string>();
        while (order.Count < 5)
        {
            var got = await Client.PullAsync(c, new PullOptions { MaxMessages = 10, Expires = TimeSpan.FromMilliseconds(500) });
            await Client.AckAsync(c, got.Select(m => m.Offset));
            order.AddRange(got.Select(m => m.Text()));
        }
        Assert.Equal(new[] { "high1", "high2", "mid", "low1", "low2" }, order);
    }

    [E2EFact]
    public async Task SingleActiveConsumersRefusePullsAndFailOver()
    {
        var s = Uniq("single");
        await Client.CreateStreamAsync(s);
        var c = Uniq("single-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { SingleActive = true });
        Assert.True((await Client.ConsumerInfoAsync(c)).Spec.SingleActive);
        await Assert.ThrowsAsync<ExspeedServerException>(() => Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromMilliseconds(200) }));
        await using var other = await _f.Server.ConnectAsync();
        var first = await Client.SubscribeAsync(c);
        var second = await other.SubscribeAsync(c);
        await Client.PublishAsync(s, "x.y", "1");
        var m = (await first.NextAsync(TimeSpan.FromSeconds(5)))!;
        m.Ack();
        Assert.Null(await second.NextAsync(TimeSpan.FromMilliseconds(300)));
        await first.UnsubscribeAsync();
        await Client.PublishAsync(s, "x.y", "2");
        Assert.Equal("2", (await second.NextAsync(TimeSpan.FromSeconds(5)))!.Text());
        await second.UnsubscribeAsync();
    }

    // ---- core pub/sub -----------------------------------------------------------------

    [E2EFact]
    public async Task FansOutToEveryMatchingSubscriptionAndStoresNothing()
    {
        await using var a = await _f.Server.ConnectAsync();
        await using var b = await _f.Server.ConnectAsync();
        var all = await a.SubscribeCoreAsync("orders.>");
        var eu = await b.SubscribeCoreAsync("orders.eu.*");
        await Client.PublishCoreAsync("orders.eu.created", "{\"id\":1}", new CorePublishOptions { Headers = new[] { H("trace-id", "t1") } });
        await Client.PublishCoreAsync("orders.us.created", "2");
        await Client.PublishCoreAsync("billing.x", "ignored");

        var m1 = (await all.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal(("orders.eu.created", "{\"id\":1}", "t1", (string?)null), (m1.Subject, m1.Text(), m1.Header("trace-id"), m1.ReplyTo));
        Assert.Equal("orders.us.created", (await all.NextAsync(TimeSpan.FromSeconds(5)))!.Subject);
        Assert.Equal("orders.eu.created", (await eu.NextAsync(TimeSpan.FromSeconds(5)))!.Subject);
        Assert.Null(await eu.NextAsync(TimeSpan.FromMilliseconds(200)));
        Assert.Null(await all.NextAsync(TimeSpan.FromMilliseconds(200)));

        // A late subscriber sees only what comes next.
        var late = await a.SubscribeCoreAsync("orders.>");
        Assert.Null(await late.NextAsync(TimeSpan.FromMilliseconds(200)));
        await late.UnsubscribeAsync();
        await all.UnsubscribeAsync();
        await Client.PublishCoreAsync("orders.eu.created", "3");
        Assert.Equal("3", (await eu.NextAsync(TimeSpan.FromSeconds(5)))!.Text());
        Assert.True(all.IsClosed);
    }

    [E2EFact]
    public async Task SplitsMessagesAcrossAQueueGroup()
    {
        await using var w1 = await _f.Server.ConnectAsync();
        await using var w2 = await _f.Server.ConnectAsync();
        var subject = $"jobs.{Uniq("q")}";
        var s1 = await w1.SubscribeCoreAsync(subject, new CoreSubscribeOptions { Queue = "workers" });
        var s2 = await w2.SubscribeCoreAsync(subject, new CoreSubscribeOptions { Queue = "workers" });
        for (int i = 0; i < 20; i++)
        {
            await Client.PublishCoreAsync(subject, i.ToString());
        }
        static async Task<int> Count(CoreSubscription s)
        {
            int n = 0;
            while (await s.NextAsync(TimeSpan.FromMilliseconds(300)) is not null)
            {
                n++;
            }
            return n;
        }
        var (n1, n2) = (await Count(s1), await Count(s2));
        Assert.Equal(20, n1 + n2);
        Assert.True(n1 > 0);
        Assert.True(n2 > 0);
    }

    // ---- request-reply ------------------------------------------------------------------

    [E2EFact]
    public async Task AnswersRequestsThroughOneInboxAndFailsFastWithNoResponders()
    {
        await using var svc = await _f.Server.ConnectAsync();
        var reqs = await svc.SubscribeCoreAsync("svc.upper", new CoreSubscribeOptions { Queue = "svc" });
        var responder = Task.Run(async () =>
        {
            await foreach (var m in reqs)
            {
                await m.RespondAsync(m.Text().ToUpperInvariant());
            }
        });

        var r = await Client.RequestAsync("svc.upper", "hello", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) });
        Assert.Equal("HELLO", r.Text());
        var many = await Task.WhenAll(Enumerable.Range(0, 20).Select(i => Client.RequestAsync("svc.upper", $"m{i}", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) })));
        Assert.Equal(Enumerable.Range(0, 20).Select(i => $"M{i}"), many.Select(m => m.Text()));

        var sw = Stopwatch.StartNew();
        var err = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.RequestAsync("svc.nobody", "x", new CoreRequestOptions { Timeout = TimeSpan.FromSeconds(5) }));
        Assert.Equal(404, err.Code);
        Assert.True(sw.ElapsedMilliseconds < 2000);

        await reqs.UnsubscribeAsync();
        await responder;
    }

    [E2EFact]
    public async Task TimesOutWhenAResponderNeverAnswers()
    {
        await using var svc = await _f.Server.ConnectAsync();
        var subject = $"svc.{Uniq("silent")}";
        var silent = await svc.SubscribeCoreAsync(subject);
        await Assert.ThrowsAsync<ExspeedTimeoutException>(() => Client.RequestAsync(subject, "x", new CoreRequestOptions { Timeout = TimeSpan.FromMilliseconds(300) }));
        Assert.StartsWith("_INBOX.", (await silent.NextAsync(TimeSpan.FromSeconds(1)))!.ReplyTo);
    }

    // ---- kv buckets -------------------------------------------------------------------

    [E2EFact]
    public async Task PutsGetsComparesAndSetsDeletesAndListsKeys()
    {
        var kv = Client.Kv(Uniq("cfg"));
        await kv.CreateAsync(new KvBucketOptions { History = 3 });
        await kv.CreateAsync(new KvBucketOptions { History = 3 }); // idempotent
        Assert.Null(await kv.GetAsync("app.mode"));

        var r1 = await kv.PutAsync("app.mode", "dev");
        var r2 = await kv.PutAsync("app.mode", "{\"mode\":\"prod\"}");
        Assert.Equal((1UL, 2UL), (r1, r2));
        var e = (await kv.GetAsync("app.mode"))!;
        Assert.Equal(("app.mode", r2, KvOp.Put, "prod"), (e.Key, e.Revision, e.Op, e.Json<Dictionary<string, string>>()!["mode"]));
        Assert.Equal("dev", (await kv.GetRevisionAsync("app.mode", r1))!.Text());

        // Compare-and-set.
        var created = await kv.CreateKeyAsync("app.port", "8080");
        Assert.Equal(409, (await Assert.ThrowsAsync<ExspeedServerException>(() => kv.CreateKeyAsync("app.port", "9090"))).Code);
        var updated = await kv.UpdateAsync("app.port", "9090", created);
        var stale = await Assert.ThrowsAsync<ExspeedServerException>(() => kv.UpdateAsync("app.port", "1", created));
        Assert.Equal(409, stale.Code);
        Assert.Equal(updated, stale.Detail!.Value.GetProperty("current_revision").GetUInt64());

        await kv.PutAsync("db.url", "postgres://");
        Assert.Equal(new[] { "app.mode", "app.port", "db.url" }, await kv.KeysAsync());
        Assert.Equal(new[] { "app.mode", "app.port" }, await kv.KeysAsync("app.*"));

        var del = await kv.DeleteAsync("app.port");
        Assert.True(del > updated);
        Assert.Null(await kv.GetAsync("app.port"));
        Assert.Equal(new[] { "app.mode" }, await kv.KeysAsync("app.*"));
        Assert.Equal(new[] { ("8080", KvOp.Put), ("9090", KvOp.Put), ("", KvOp.Delete) }, (await kv.HistoryAsync("app.port")).Select(h => (h.Text(), h.Op)));
        // A deleted key can be created again.
        Assert.True(await kv.CreateKeyAsync("app.port", "7070") > del);

        await kv.PurgeAsync("app.mode");
        Assert.Null(await kv.GetAsync("app.mode"));

        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.Kv(Uniq("missing")).GetAsync("x"))).Code);

        await kv.DestroyAsync();
        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.StreamInfoAsync(kv.Stream))).Code);
    }

    [E2EFact]
    public async Task ExpiresKeysAfterTheirTtl()
    {
        var kv = Client.Kv(Uniq("ttl"));
        await kv.CreateAsync();
        await kv.PutAsync("session.a", "x", new KvPutOptions { Ttl = TimeSpan.FromMilliseconds(200) });
        await kv.PutAsync("session.b", "y");
        await Task.Delay(500);
        Assert.Null(await kv.GetAsync("session.a"));
        Assert.Equal("y", (await kv.GetAsync("session.b"))!.Text());
    }

    [E2EFact]
    public async Task WatchesCurrentValuesFirstThenEveryChange()
    {
        var kv = Client.Kv(Uniq("watch"));
        await kv.CreateAsync();
        await kv.PutAsync("user.1", "alice");
        await kv.PutAsync("user.2", "bob");
        await kv.PutAsync("user.1", "alice2");
        await kv.PutAsync("other.x", "filtered");
        await kv.PutAsync("user.3", "gone");
        await kv.DeleteAsync("user.3");

        await using var w = kv.Watch("user.*");
        async Task<List<KvEntry>> Take(int n)
        {
            var output = new List<KvEntry>();
            while (output.Count < n)
            {
                var e = await w.NextAsync(TimeSpan.FromSeconds(5)) ?? throw new Xunit.Sdk.XunitException($"watch stalled after {output.Count} entries");
                output.Add(e);
            }
            return output;
        }
        var snapshot = await Take(2);
        Assert.Equal(new[] { ("user.2", "bob", 2UL), ("user.1", "alice2", 3UL) }, snapshot.Select(e => (e.Key, e.Text(), e.Revision)));

        await kv.PutAsync("user.4", "dave");
        await kv.PutAsync("other.y", "filtered");
        await kv.DeleteAsync("user.2");
        var changes = await Take(2);
        Assert.Equal(new[] { ("user.4", KvOp.Put), ("user.2", KvOp.Delete) }, changes.Select(e => (e.Key, e.Op)));
        Assert.Null(await w.NextAsync(TimeSpan.FromMilliseconds(200)));
        w.Stop();
        Assert.Null(await w.NextAsync());
    }
}
