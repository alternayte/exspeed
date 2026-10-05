using System.Diagnostics;
using System.Text.Json;
using static Exspeed.Tests.E2E.TestServer;

namespace Exspeed.Tests.E2E;

/// <summary>Streams, publishing, reads, consumers and queries against a real server.</summary>
[Collection("e2e")]
public sealed class StreamsAndConsumersE2ETests : IClassFixture<ServerFixture>
{
    private readonly ServerFixture _f;

    public StreamsAndConsumersE2ETests(ServerFixture f)
    {
        _f = f;
    }

    private ExspeedClient Client => _f.Client;

    private async Task<string> StreamAsync(string prefix = "s")
    {
        var name = Uniq(prefix);
        await Client.CreateStreamAsync(name);
        return name;
    }

    private Task PublishNAsync(string stream, int n, string subject = "work.item") =>
        Client.PublishBatchAsync(stream, Enumerable.Range(0, n).Select(i => PublishRecord.Json(subject, new { i })));

    private static int I(StreamRecord r) => r.Json<JsonElement>().GetProperty("i").GetInt32();

    // ---- basics -------------------------------------------------------------------

    [E2EFact]
    public async Task PingsAndReportsMetadata()
    {
        Assert.True(await Client.PingAsync() >= TimeSpan.Zero);
        var md = await Client.MetadataAsync();
        Assert.True(md.IsLeader);
        Assert.Equal(Client.ServerInfo.ServerVersion, md.ServerVersion);
        Assert.Equal(Client.ServerInfo.NodeId, md.NodeId);
        Assert.Null(Client.ServerInfo.Leader);
    }

    [E2EFact]
    public async Task ManagesStreams()
    {
        var name = Uniq("admin");
        await Client.CreateStreamAsync(new StreamSpec(name) { MaxAge = TimeSpan.FromHours(1) });
        await Client.CreateStreamAsync(new StreamSpec(name) { MaxAge = TimeSpan.FromHours(1) }); // same settings: ok
        var conflict = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.CreateStreamAsync(new StreamSpec(name) { MaxAge = TimeSpan.FromMinutes(1) }));
        Assert.Equal(409, conflict.Code);

        await Client.PublishAsync(name, PublishRecord.Json("admin.created", new { n = 1 }));
        var info = await Client.StreamInfoAsync(name);
        Assert.Equal((name, 0UL, 1UL, 1UL, false), (info.Name, info.EarliestOffset, info.NextOffset, info.Records, info.Internal));
        Assert.Equal(TimeSpan.FromHours(1), info.Config.MaxAge);
        Assert.Contains(name, (await Client.ListStreamsAsync()).Select(s => s.Name));

        await Client.UpdateStreamAsync(new StreamSpec(name) { MaxAge = TimeSpan.FromHours(2) });
        Assert.Equal(TimeSpan.FromHours(2), (await Client.StreamInfoAsync(name)).Config.MaxAge);

        var c = Uniq("admin-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, name));
        var busy = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.DeleteStreamAsync(name));
        Assert.Equal(409, busy.Code);
        Assert.Equal(c, busy.Detail!.Value.GetProperty("consumers")[0].GetString());
        await Client.DeleteConsumerAsync(c);
        await Client.DeleteStreamAsync(name);
        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.StreamInfoAsync(name))).Code);
    }

    [E2EFact]
    public async Task RejectsInternalStreamNamesAndUnknownStreams()
    {
        Assert.Equal(403, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.CreateStreamAsync("__nope"))).Code);
        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.PublishAsync(Uniq("missing"), "x.y", "v"))).Code);
    }

    // ---- publishing and reading ---------------------------------------------------------

    [E2EFact]
    public async Task PublishesAndReadsBackWithSubjectFilters()
    {
        var s = await StreamAsync("orders");
        var subjects = new[] { "orders.placed", "orders.shipped", "orders.eu.placed", "payments.done", "orders.placed" };
        for (int i = 0; i < subjects.Length; i++)
        {
            var r = await Client.PublishAsync(s, PublishRecord.Json(subjects[i], new { i }) with
            {
                Key = System.Text.Encoding.UTF8.GetBytes($"k{i}"),
                Headers = new[] { new KeyValuePair<string, string>("x-index", i.ToString()) },
            });
            Assert.Equal(new PublishResult((ulong)i, false), r);
        }

        var all = await Client.ReadAsync(s);
        Assert.Equal(subjects, all.Records.Select(r => r.Subject));
        Assert.Equal((5UL, 5UL), (all.NextOffset, all.HighWatermark));
        var first = all.Records[0];
        Assert.Equal(0, I(first));
        Assert.Equal("k0", first.KeyText());
        Assert.Equal("0", first.Header("x-index"));
        Assert.True(first.Timestamp > DateTimeOffset.UtcNow.AddMinutes(-1));

        Assert.Equal(new ulong[] { 0, 1, 4 }, (await Client.ReadAsync(s, new ReadOptions { Filter = "orders.*" })).Records.Select(r => r.Offset));
        Assert.Equal(new ulong[] { 0, 1, 2, 4 }, (await Client.ReadAsync(s, new ReadOptions { Filter = "orders.>" })).Records.Select(r => r.Offset));
        var page = await Client.ReadAsync(s, new ReadOptions { From = 1, MaxRecords = 2 });
        Assert.Equal(new ulong[] { 1, 2 }, page.Records.Select(r => r.Offset));
        Assert.Equal(3UL, page.NextOffset);

        var bad = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.ReadAsync(s, new ReadOptions { Filter = "orders.>.x" }));
        Assert.Equal(400, bad.Code);
    }

    [E2EFact]
    public async Task LongPollsAReadUntilNewDataArrives()
    {
        var s = await StreamAsync();
        var sw = Stopwatch.StartNew();
        var pending = Client.ReadAsync(s, new ReadOptions { Wait = TimeSpan.FromSeconds(5) });
        await Task.Delay(200);
        await Client.PublishAsync(s, "late.arrival", "hello");
        var r = await pending;
        Assert.Equal(new[] { "hello" }, r.Records.Select(x => x.Text()));
        Assert.True(sw.ElapsedMilliseconds < 4000);
    }

    [E2EFact]
    public async Task PublishesBatchesAndDeduplicatesByMsgId()
    {
        var s = await StreamAsync();
        var (m1, m2, m3) = (MsgId.New(), MsgId.New(), MsgId.New());
        var first = await Client.PublishBatchAsync(s, new[]
        {
            PublishRecord.Json("orders.placed", new { id = 1 }) with { MsgId = m1 },
            PublishRecord.Json("orders.placed", new { id = 2 }) with { MsgId = m2 },
        });
        Assert.Equal(new[] { new PublishResult(0, false), new PublishResult(1, false) }, first);
        var retry = await Client.PublishBatchAsync(s, new[]
        {
            PublishRecord.Json("orders.placed", new { id = 1 }) with { MsgId = m1 },
            PublishRecord.Json("orders.placed", new { id = 3 }) with { MsgId = m3 },
        });
        Assert.Equal(new[] { new PublishResult(0, true), new PublishResult(2, false) }, retry);
        Assert.Equal(new PublishResult(1, true), await Client.PublishAsync(s, PublishRecord.Json("orders.placed", new { id = 2 }) with { MsgId = m2 }));

        var reused = await Assert.ThrowsAsync<ExspeedServerException>(() => Client.PublishAsync(s, PublishRecord.Json("orders.placed", new { id = 99 }) with { MsgId = m1 }));
        Assert.Equal(409, reused.Code);
        Assert.Equal(0, reused.Detail!.Value.GetProperty("stored_offset").GetInt32());
        Assert.Equal(3UL, (await Client.StreamInfoAsync(s)).NextOffset);
    }

    [E2EFact]
    public async Task KeepsTheCoalescingPublishersRecordsInCallOrder()
    {
        var s = await StreamAsync();
        await using var p = Client.CreatePublisher(new PublisherOptions { MaxBatchRecords = 64 });
        const int n = 1000;
        var results = await Task.WhenAll(Enumerable.Range(0, n).Select(i => p.PublishAsync(s, PublishRecord.Json("seq.value", new { i }))));
        Assert.Equal(Enumerable.Range(0, n).Select(i => (ulong)i), results.Select(r => r.Offset));
        await p.CloseAsync();

        var seen = new List<int>();
        ulong from = 0;
        while (seen.Count < n)
        {
            var r = await Client.ReadAsync(s, new ReadOptions { From = from, MaxRecords = 500 });
            seen.AddRange(r.Records.Select(I));
            from = r.NextOffset;
        }
        Assert.Equal(Enumerable.Range(0, n), seen);
    }

    // ---- consumers --------------------------------------------------------------------

    [E2EFact]
    public async Task CreatesAConsumerSubscribesReceivesAndAcks()
    {
        var s = await StreamAsync();
        var c = Uniq("billing");
        var info = await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { FilterSubjects = new[] { "work.>" } });
        Assert.Equal((c, s), (info.Spec.Name, info.Spec.Stream));
        Assert.Equal(new[] { "work.>" }, info.Spec.FilterSubjects);
        Assert.Equal(DeliverPolicy.All, info.Spec.Deliver);
        Assert.Equal(AckPolicy.Explicit, info.Spec.Ack);
        // Idempotent for the same spec; 409 for a different one.
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { FilterSubjects = new[] { "work.>" } });
        Assert.Equal(409, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.CreateConsumerAsync(new ConsumerSpec(c, s)))).Code);
        Assert.Equal(new[] { c }, (await Client.ListConsumersAsync(s)).Select(i => i.Spec.Name));

        await PublishNAsync(s, 3);
        await Client.PublishAsync(s, "other.thing", "filtered out");
        var sub = await Client.SubscribeAsync(c, new SubscribeOptions { Window = 10 });
        var got = new List<Message>();
        await foreach (var m in sub)
        {
            got.Add(m);
            m.Ack();
            if (got.Count == 3)
            {
                break;
            }
        }
        Assert.Equal(new[] { (0UL, 1, 0), (1UL, 1, 1), (2UL, 1, 2) }, got.Select(m => (m.Offset, m.DeliveryCount, I(m))));
        Assert.Equal(new EndReason(0, "unsubscribed"), sub.EndReason);
        var after = await Eventually(async () =>
        {
            var i = await Client.ConsumerInfoAsync(c);
            return i.NumUnacked == 0 && i.Stats.Acked == 3 ? i : null;
        });
        Assert.True(after.AckFloor >= 3);
    }

    [E2EFact]
    public async Task NeverPushesMoreThanTheCreditWindow()
    {
        var s = await StreamAsync();
        var c = Uniq("credit");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await PublishNAsync(s, 50);
        var sub = await Client.SubscribeAsync(c, new SubscribeOptions { Window = 4 });
        await Eventually(() => Task.FromResult(sub.Buffered == 4));
        await Task.Delay(300);
        Assert.Equal(4, sub.Buffered); // nothing beyond the window

        var offsets = new List<ulong>();
        while (offsets.Count < 50)
        {
            var m = await sub.NextAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(m);
            Assert.True(sub.Buffered <= 4);
            offsets.Add(m!.Offset);
            m.Ack();
        }
        Assert.Equal(Enumerable.Range(0, 50).Select(i => (ulong)i), offsets);
        await sub.UnsubscribeAsync();
    }

    [E2EFact]
    public async Task RedeliversANackedMessageWithAHigherDeliveryCount()
    {
        var s = await StreamAsync();
        var c = Uniq("nack");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await Client.PublishAsync(s, "work.item", "retry me");
        await using var sub = await Client.SubscribeAsync(c, new SubscribeOptions { Window = 10 });
        var first = (await sub.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal(1, first.DeliveryCount);
        await first.NackAsync();
        var second = (await sub.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal(first.Offset, second.Offset);
        Assert.Equal(2, second.DeliveryCount);
        await second.NackAsync(TimeSpan.FromMilliseconds(200));
        var sw = Stopwatch.StartNew();
        var third = (await sub.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal(3, third.DeliveryCount);
        Assert.True(sw.ElapsedMilliseconds >= 150, $"redelivered after {sw.ElapsedMilliseconds} ms");
        await third.InProgressAsync();
        await Client.AckAsync(c, new[] { third.Offset }); // confirmed ack
        Assert.Equal(0UL, (await Client.ConsumerInfoAsync(c)).NumUnacked);
    }

    [E2EFact]
    public async Task RedeliversWhenTheAckWaitPasses()
    {
        var s = await StreamAsync();
        var c = Uniq("ackwait");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { AckWait = TimeSpan.FromMilliseconds(300) });
        await Client.PublishAsync(s, "work.item", "slow");
        var first = Assert.Single(await Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromSeconds(2) }));
        Assert.Equal(1, first.DeliveryCount);
        var again = await Eventually(async () => (await Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromSeconds(1) })).FirstOrDefault());
        Assert.Equal((first.Offset, 2), (again.Offset, again.DeliveryCount));
        again.Ack();
    }

    [E2EFact]
    public async Task SharesWorkBetweenTwoClientsOnOneConsumer()
    {
        var s = await StreamAsync();
        var c = Uniq("shared");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await using var other = await _f.Server.ConnectAsync(new ExspeedClientOptions { ClientId = "e2e-2", Reconnect = null });
        var subA = await Client.SubscribeAsync(c, new SubscribeOptions { Window = 8 });
        var subB = await other.SubscribeAsync(c, new SubscribeOptions { Window = 8 });
        var seen = new Dictionary<string, List<ulong>> { ["a"] = new(), ["b"] = new() };
        int total = 200;
        async Task Drain(string name, Subscription sub)
        {
            await foreach (var m in sub)
            {
                Assert.Equal(1, m.DeliveryCount);
                lock (seen)
                {
                    seen[name].Add(m.Offset);
                }
                m.Ack();
                await Task.Delay(1); // let the other subscriber get a share
            }
        }
        var done = Task.WhenAll(Drain("a", subA), Drain("b", subB));
        await PublishNAsync(s, total);
        await Eventually(() => Task.FromResult(seen.Values.Sum(v => v.Count) >= total), 15_000);
        await Task.Delay(200); // anything extra would show up now
        await subA.UnsubscribeAsync();
        await subB.UnsubscribeAsync();
        await done;
        Assert.NotEmpty(seen["a"]);
        Assert.NotEmpty(seen["b"]);
        Assert.Equal(Enumerable.Range(0, total).Select(i => (ulong)i), seen["a"].Concat(seen["b"]).OrderBy(x => x));
    }

    [E2EFact]
    public async Task DeadLettersAfterMaxDeliverAndImmediatelyOnTerm()
    {
        var s = await StreamAsync();
        var dlq = await StreamAsync("dlq");
        var c = Uniq("dlq-c");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { MaxDeliver = 2, DlqStream = dlq });
        await Client.PublishAsync(s, "work.poison", "bad");
        await Client.PublishAsync(s, "work.terminal", "worse");
        var one = new PullOptions { MaxMessages = 1, Expires = TimeSpan.FromSeconds(2) };

        var m1 = Assert.Single(await Client.PullAsync(c, one));
        Assert.Equal(1, m1.DeliveryCount);
        await m1.NackAsync();
        var m2 = Assert.Single(await Client.PullAsync(c, one));
        Assert.Equal((0UL, 2), (m2.Offset, m2.DeliveryCount));
        await m2.NackAsync();

        var t = Assert.Single(await Client.PullAsync(c, one));
        Assert.Equal(1UL, t.Offset);
        await t.TermAsync("cannot parse");

        var dead = await Eventually(async () =>
        {
            var r = await Client.ReadAsync(dlq);
            return r.Records.Count == 2 ? r.Records : null;
        });
        Assert.Equal(new[] { "bad", "worse" }, dead.Select(d => d.Text()));
        Assert.Equal(c, dead[0].Header("exspeed-dlq-origin"));
        Assert.Equal(s, dead[0].Header("exspeed-dlq-stream"));
        Assert.Equal("0", dead[0].Header("exspeed-dlq-original-offset"));
        Assert.Equal("2", dead[0].Header("exspeed-dlq-deliveries"));
        Assert.Equal("1", dead[1].Header("exspeed-dlq-original-offset"));
        Assert.Contains("cannot parse", dead[1].Header("exspeed-dlq-reason"));
        Assert.Equal(2UL, (await Client.ConsumerInfoAsync(c)).Stats.DeadLettered);
        Assert.Empty(await Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromMilliseconds(300) }));
    }

    [E2EFact]
    public async Task LongPollsAPullUntilAMessageArrivesAndTimesOutEmpty()
    {
        var s = await StreamAsync();
        var c = Uniq("pull");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));

        var sw = Stopwatch.StartNew();
        Assert.Empty(await Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromMilliseconds(300) }));
        Assert.True(sw.ElapsedMilliseconds >= 250);

        sw.Restart();
        var pending = Client.PullAsync(c, new PullOptions { MaxMessages = 10, Expires = TimeSpan.FromSeconds(10) });
        await Task.Delay(200);
        await Client.PublishAsync(s, "work.item", "now");
        var msgs = await pending;
        Assert.Equal(new[] { "now" }, msgs.Select(m => m.Text()));
        Assert.True(sw.ElapsedMilliseconds < 5000);
        // A long pull doesn't block other requests on the same connection.
        var slow = Client.PullAsync(c, new PullOptions { Expires = TimeSpan.FromSeconds(1) });
        Assert.True(await Client.PingAsync() < TimeSpan.FromMilliseconds(500));
        await slow;
        msgs[0].Ack();
    }

    [E2EFact]
    public async Task SeeksAConsumerToAnOffsetTheStartTheEndAndATime()
    {
        var s = await StreamAsync();
        var c = Uniq("seek");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s) { Ack = AckPolicy.None });
        await PublishNAsync(s, 5);
        async Task<ulong[]> Offsets() =>
            (await Client.PullAsync(c, new PullOptions { MaxMessages = 100, Expires = TimeSpan.FromMilliseconds(300) })).Select(m => m.Offset).ToArray();

        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, await Offsets());
        await Client.SeekAsync(c, SeekTarget.Offset(2));
        Assert.Equal(new ulong[] { 2, 3, 4 }, await Offsets());
        await Client.SeekAsync(c, SeekTarget.Earliest);
        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, await Offsets());
        await Client.SeekAsync(c, SeekTarget.Latest);
        Assert.Empty(await Offsets());
        await Client.SeekAsync(c, SeekTarget.TimeMs(0));
        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, await Offsets());
        await Client.SeekAsync(c, SeekTarget.Time(DateTimeOffset.UtcNow.AddMinutes(1)));
        Assert.Empty(await Offsets());
        Assert.Equal(404, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.SeekAsync(Uniq("nobody"), SeekTarget.Earliest))).Code);
    }

    [E2EFact]
    public async Task StartsConsumersAtTheRequestedDeliverPolicy()
    {
        var s = await StreamAsync();
        await PublishNAsync(s, 3);
        var fromOffset = Uniq("from");
        await Client.CreateConsumerAsync(new ConsumerSpec(fromOffset, s) { Deliver = DeliverPolicy.FromOffset(1), Ack = AckPolicy.None });
        Assert.Equal(new ulong[] { 1, 2 }, (await Client.PullAsync(fromOffset, new PullOptions { Expires = TimeSpan.FromMilliseconds(300) })).Select(m => m.Offset));
        var onlyNew = Uniq("new");
        await Client.CreateConsumerAsync(new ConsumerSpec(onlyNew, s) { Deliver = DeliverPolicy.New, Ack = AckPolicy.None });
        await Client.PublishAsync(s, "work.item", "fresh");
        Assert.Equal(new[] { "fresh" }, (await Client.PullAsync(onlyNew, new PullOptions { Expires = TimeSpan.FromMilliseconds(500) })).Select(m => m.Text()));
    }

    [E2EFact]
    public async Task RemovesAnEphemeralConsumerWhenItsConnectionCloses()
    {
        var s = await StreamAsync();
        var c = Uniq("eph");
        var owner = await _f.Server.ConnectAsync();
        await owner.CreateConsumerAsync(new ConsumerSpec(c, s) { Ephemeral = true, Deliver = DeliverPolicy.New });
        Assert.True((await Client.ConsumerInfoAsync(c)).Spec.Ephemeral);
        await owner.CloseAsync();
        var err = await Eventually(async () =>
        {
            try
            {
                await Client.ConsumerInfoAsync(c);
                return null;
            }
            catch (ExspeedServerException e)
            {
                return e;
            }
        });
        Assert.Equal(404, err.Code);
    }

    [E2EFact]
    public async Task EndsSubscriptionsWith404WhenTheConsumerIsDeleted()
    {
        var s = await StreamAsync();
        var c = Uniq("gone");
        await Client.CreateConsumerAsync(new ConsumerSpec(c, s));
        var sub = await Client.SubscribeAsync(c);
        var next = sub.NextAsync();
        await Client.DeleteConsumerAsync(c);
        Assert.Null(await next.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(404, sub.EndReason?.Code);
    }

    [E2EFact]
    public async Task RunsSqlQueries()
    {
        var s = Uniq("q").Replace('-', '_');
        await Client.CreateStreamAsync(s);
        await Client.PublishBatchAsync(s, new[] { 1, 2, 3 }.Select(i => PublishRecord.Json("metrics.cpu", new { region = i == 2 ? "us" : "eu", i })));
        var r = await Client.QueryAsync($"SELECT COUNT(*) AS cnt FROM \"{s}\"");
        Assert.Equal(new[] { "cnt" }, r.Columns);
        Assert.Equal(3, Assert.Single(Assert.Single(r.Rows)).GetInt32());
        Assert.Equal(1UL, r.RowCount);
        Assert.True(r.ExecutionTimeMs >= 0);
        Assert.Equal(400, (await Assert.ThrowsAsync<ExspeedServerException>(() => Client.QueryAsync("SELEKT nonsense"))).Code);
    }
}
