using Exspeed.Protocol;
using static Exspeed.Tests.ClientTests;

namespace Exspeed.Tests;

/// <summary>The coalescing publisher against the fake server.</summary>
public class PublisherTests
{
    /// <summary>A handler that acknowledges publishes with increasing offsets.</summary>
    private sealed class AutoAck
    {
        private long _next;

        public bool Handle(FakeConn conn, uint corr, Request req)
        {
            switch (req)
            {
                case Request.Publish:
                    conn.Reply(corr, new Response.PublishOk((ulong)(Interlocked.Increment(ref _next) - 1), false));
                    return true;
                case Request.PublishBatch pb:
                    var results = pb.Records.Select(_ => ((ulong)(Interlocked.Increment(ref _next) - 1), false)).ToList();
                    conn.Reply(corr, new Response.PublishBatchOk(results));
                    return true;
                default:
                    return false;
            }
        }
    }

    private static List<string> Subjects(IEnumerable<Request> reqs) => reqs.SelectMany(r => r switch
    {
        Request.PublishBatch b => b.Records.Select(x => x.Subject),
        Request.Publish p => new[] { p.Record.Subject },
        _ => Enumerable.Empty<string>(),
    }).ToList();

    private static List<Request> Sent(FakeServer s) => s.Last.Received.Skip(1).Select(r => r.Req).ToList();

    [Fact]
    public async Task CoalescesConcurrentPublishesIntoBatchesInCallOrder()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher();
        var tasks = Enumerable.Range(0, 100).Select(i => p.PublishAsync("s", new PublishRecord($"n.{i}", i.ToString()))).ToList();
        var results = await Task.WhenAll(tasks);
        Assert.Equal(Enumerable.Range(0, 100).Select(i => (ulong)i), results.Select(r => r.Offset));
        var sent = Sent(server);
        Assert.True(sent.Count < 100, $"{sent.Count} requests for 100 records");
        Assert.Contains(sent, r => r is Request.PublishBatch);
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"n.{i}"), Subjects(sent));
    }

    [Fact]
    public async Task SendsALoneRecordAsAPlainPublish()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var r = await c.CreatePublisher().PublishAsync("s", new PublishRecord("one", "x"));
        Assert.Equal(new PublishResult(0, false), r);
        Assert.IsType<Request.Publish>(server.Last.Received[1].Req);
    }

    [Fact]
    public async Task SplitsByMaxBatchRecordsAndByStreamKeepingOrder()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        // A long window, so only a full queue triggers a send.
        var p = c.CreatePublisher(new PublisherOptions { MaxBatchRecords = 3, BatchWindow = TimeSpan.FromMilliseconds(200) });
        var calls = new List<Task<PublishResult>>();
        for (int i = 0; i < 2; i++)
        {
            calls.Add(p.PublishAsync("a", new PublishRecord($"a.{i}", "")));
        }
        calls.Add(p.PublishAsync("b", new PublishRecord("b.0", ""))); // the queue is full now: [a a b]
        for (int i = 2; i < 4; i++)
        {
            calls.Add(p.PublishAsync("a", new PublishRecord($"a.{i}", "")));
        }
        await Task.WhenAll(calls);
        var sent = Sent(server);
        var shape = sent.Select(r => r switch
        {
            Request.PublishBatch b => ($"batch", b.Stream, b.Records.Count),
            Request.Publish one => ("publish", one.Stream, 1),
            _ => ("?", "", 0),
        }).ToList();
        // A full queue is flushed at once, split into same-stream runs; the rest goes when the window ends.
        Assert.Equal(new[] { ("batch", "a", 2), ("publish", "b", 1), ("batch", "a", 2) }, shape);
        Assert.Equal(new[] { "a.0", "a.1", "b.0", "a.2", "a.3" }, Subjects(sent));
    }

    [Fact]
    public async Task WaitsForABatchWindowBeforeSending()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher(new PublisherOptions { BatchWindow = TimeSpan.FromMilliseconds(50) });
        var a = p.PublishAsync("s", new PublishRecord("x", "1"));
        await Task.Delay(5);
        var b = p.PublishAsync("s", new PublishRecord("y", "2"));
        await Task.WhenAll(a, b);
        Assert.Equal(new[] { "PublishBatch" }, Sent(server).Select(r => r.GetType().Name));
    }

    [Fact]
    public async Task BoundsRecordsInFlightAndKeepsOrderWhileWaiting()
    {
        var held = new List<(FakeConn, uint, Request)>();
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.Publish or Request.PublishBatch)
            {
                lock (held)
                {
                    held.Add((conn, corr, req));
                }
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher(new PublisherOptions { MaxInFlight = 2 });
        var all = Enumerable.Range(0, 5).Select(i => p.PublishAsync("s", new PublishRecord($"n.{i}", ""))).ToList();
        await FakeServer.Until(() => held.Count >= 1);
        await Task.Delay(50);
        Assert.Equal(2, Subjects(held.Select(h => h.Item3)).Count); // first two only
        Assert.Equal(5, p.Pending);
        var ack = new AutoAck();
        while (!all.All(t => t.IsCompleted))
        {
            (FakeConn, uint, Request)? next = null;
            lock (held)
            {
                if (held.Count > 0)
                {
                    next = held[0];
                    held.RemoveAt(0);
                }
            }
            if (next is { } n)
            {
                ack.Handle(n.Item1, n.Item2, n.Item3);
            }
            await Task.Delay(10);
        }
        await Task.WhenAll(all);
        Assert.Equal(new[] { "n.0", "n.1", "n.2", "n.3", "n.4" }, Subjects(server.Last.Received.Select(r => r.Req)));
        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, all.Select(t => t.Result.Offset));
        Assert.Equal(0, p.Pending);
    }

    [Fact]
    public async Task FailsEveryRecordOfAFailedBatchWithTheServersError()
    {
        await using var server = FakeServer.Start((conn, corr, req) =>
        {
            if (req is Request.PublishBatch or Request.Publish)
            {
                conn.Reply(corr, new Response.Error(403, "forbidden", null));
                return true;
            }
            return false;
        });
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher(new PublisherOptions { BatchWindow = TimeSpan.FromMilliseconds(20) });
        var a = p.PublishAsync("s", new PublishRecord("a", ""));
        var b = p.PublishAsync("s", new PublishRecord("b", ""));
        Assert.Equal(403, (await Assert.ThrowsAsync<ExspeedServerException>(() => a)).Code);
        Assert.Equal(403, (await Assert.ThrowsAsync<ExspeedServerException>(() => b)).Code);
        await p.FlushAsync();
        Assert.Equal(0, p.Pending);
    }

    [Fact]
    public async Task FailsRecordsWithConnectionExceptionWhenNotConnected()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher();
        await c.CloseAsync();
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => p.PublishAsync("s", new PublishRecord("a", "")));
        await p.FlushAsync();
    }

    [Fact]
    public async Task FlushWaitsForEverythingAcceptedAndCloseRejectsLaterPublishes()
    {
        await using var server = FakeServer.Start(new AutoAck().Handle);
        await using var c = await ExspeedClient.ConnectAsync(Opts(server));
        var p = c.CreatePublisher();
        var tasks = Enumerable.Range(0, 10).Select(_ => p.PublishAsync("s", new PublishRecord("x", ""))).ToList();
        await p.FlushAsync();
        Assert.Equal(0, p.Pending);
        Assert.All(tasks, t => Assert.True(t.IsCompletedSuccessfully));
        await p.CloseAsync();
        var err = await Assert.ThrowsAsync<ExspeedConnectionException>(() => p.PublishAsync("s", new PublishRecord("x", "")));
        Assert.Contains("closed", err.Message);
    }
}
