using System.Text;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>What a key-value revision holds.</summary>
public enum KvOp
{
    /// <summary>A value.</summary>
    Put,

    /// <summary>A delete tombstone (<c>exspeed-kv-op: DEL</c>).</summary>
    Delete,

    /// <summary>A purge tombstone (<c>exspeed-kv-op: PURGE</c>); older values are hidden too.</summary>
    Purge,
}

/// <summary>Options for <see cref="KvBucket.CreateAsync"/>.</summary>
public sealed record KvBucketOptions
{
    /// <summary>Values kept per key, 1 to 64. 0 = the default (1).</summary>
    public uint History { get; init; }

    /// <summary>Keys expire this long after their last put. <c>null</c> = never.</summary>
    public TimeSpan? Ttl { get; init; }

    /// <summary>Size limit in bytes; 0 = the server's default.</summary>
    public ulong MaxBytes { get; init; }
}

/// <summary>Options for <see cref="KvBucket.PutAsync(string, ReadOnlyMemory{byte}, KvPutOptions?, CancellationToken)"/>.</summary>
public sealed record KvPutOptions
{
    /// <summary>Expire this value this long after the put.</summary>
    public TimeSpan? Ttl { get; init; }

    /// <summary>Only if the key is at this revision (0 = absent), else <see cref="ExspeedServerException"/> 409.</summary>
    public ulong? ExpectedRevision { get; init; }
}

/// <summary>Options for <see cref="KvBucket.DeleteAsync"/> and <see cref="KvBucket.PurgeAsync"/>.</summary>
public sealed record KvDeleteOptions
{
    /// <summary>Only if the key is at this revision, else <see cref="ExspeedServerException"/> 409.</summary>
    public ulong? ExpectedRevision { get; init; }
}

/// <summary>One revision of a key.</summary>
public sealed class KvEntry
{
    internal KvEntry(WireRecord r)
    {
        Key = r.Subject;
        Value = r.Value;
        Revision = r.Offset + 1;
        TimestampNs = r.TimestampNs;
        string? op = null;
        foreach (var (k, v) in r.Headers)
        {
            if (k == HeaderNames.KvOp)
            {
                op = v;
                break;
            }
        }
        Op = op switch
        {
            "DEL" => KvOp.Delete,
            "PURGE" => KvOp.Purge,
            _ => KvOp.Put,
        };
    }

    /// <summary>The key.</summary>
    public string Key { get; }

    /// <summary>The value (empty for tombstones).</summary>
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>The record's offset in the bucket's stream plus one (0 = absent).</summary>
    public ulong Revision { get; }

    /// <summary>Write time, nanoseconds since the Unix epoch.</summary>
    public ulong TimestampNs { get; }

    /// <summary>Write time.</summary>
    public DateTimeOffset Timestamp => DateTimeOffset.UnixEpoch.AddTicks((long)(TimestampNs / 100));

    /// <summary><see cref="KvOp.Delete"/> and <see cref="KvOp.Purge"/> entries are tombstones with an empty value.</summary>
    public KvOp Op { get; }

    /// <summary>The value as UTF-8 text.</summary>
    /// <returns>The text.</returns>
    public string Text() => Encoding.UTF8.GetString(Value.Span);

    /// <summary>The value parsed as JSON.</summary>
    /// <typeparam name="T">The type to deserialize.</typeparam>
    /// <param name="options">Serializer options.</param>
    /// <returns>The deserialized value.</returns>
    public T? Json<T>(JsonSerializerOptions? options = null) => JsonSerializer.Deserialize<T>(Value.Span, options);
}

/// <summary>What a bucket needs from the client.</summary>
internal interface IKvHost
{
    Task<Response> RawRequestAsync(Request req, TimeSpan? timeout, CancellationToken cancellationToken);

    Task<Response.ReadResult> ReadRawAsync(string stream, ReadOptions options, CancellationToken cancellationToken);
}

/// <summary>
/// A key-value bucket, from <see cref="ExspeedClient.Kv"/>. The bucket <c>B</c> is the stream <c>KV_B</c>: each key
/// is a subject, each put a record, and a key's revision is its record's offset plus one.
/// </summary>
public sealed class KvBucket
{
    private readonly IKvHost _host;

    internal KvBucket(IKvHost host, string bucket)
    {
        _host = host;
        Bucket = bucket;
    }

    /// <summary>The bucket name.</summary>
    public string Bucket { get; }

    /// <summary>The bucket's stream (<c>KV_&lt;bucket&gt;</c>).</summary>
    public string Stream => $"KV_{Bucket}";

    private static T Expect<T>(Request req, Response resp)
        where T : Response =>
        resp as T ?? throw new ExspeedProtocolException($"unexpected reply to {req.Name}: {resp.Name}");

    private async Task<T> CallAsync<T>(Request req, CancellationToken cancellationToken)
        where T : Response =>
        Expect<T>(req, await _host.RawRequestAsync(req, null, cancellationToken).ConfigureAwait(false));

    /// <summary>Create the bucket. Idempotent for the same settings.</summary>
    /// <param name="options">History, TTL and size limit.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when the bucket exists.</returns>
    public async Task CreateAsync(KvBucketOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new KvBucketOptions();
        var ttl = options.Ttl is { } t ? Durations.Millis(t, "ttl") : 0;
        await CallAsync<Response.Ok>(new Request.KvCreateBucket(Bucket, options.History, ttl, options.MaxBytes), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Delete the bucket and every key in it.</summary>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when the bucket is gone.</returns>
    public Task DestroyAsync(CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.DeleteStream(Stream), cancellationToken);

    /// <summary>The current value of <paramref name="key"/>, or <c>null</c> when it is absent, deleted or expired.</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The entry, or <c>null</c>. A missing bucket throws <see cref="ExspeedServerException"/> 404.</returns>
    public Task<KvEntry?> GetAsync(string key, CancellationToken cancellationToken = default) =>
        GetAtAsync(key, null, cancellationToken);

    /// <summary><paramref name="key"/> at <paramref name="revision"/>, while the bucket still keeps it; else <c>null</c>.</summary>
    /// <param name="key">The key.</param>
    /// <param name="revision">The revision.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The entry, or <c>null</c>.</returns>
    public Task<KvEntry?> GetRevisionAsync(string key, ulong revision, CancellationToken cancellationToken = default) =>
        GetAtAsync(key, revision, cancellationToken);

    private async Task<KvEntry?> GetAtAsync(string key, ulong? revision, CancellationToken cancellationToken)
    {
        var req = new Request.KvGet(Bucket, key, revision);
        Response resp;
        try
        {
            resp = await _host.RawRequestAsync(req, null, cancellationToken).ConfigureAwait(false);
        }
        catch (ExspeedServerException e) when (e.Code == ErrorCodes.NotFound && e.Message.StartsWith("key ", StringComparison.Ordinal))
        {
            // 404 is "key '...' not found" or "bucket '...' not found"; only the first means "absent".
            return null;
        }
        var records = Expect<Response.Messages>(req, resp).Records;
        return records.Count > 0 ? new KvEntry(records[0]) : null;
    }

    /// <summary>Set <paramref name="key"/>; returns the new revision.</summary>
    /// <param name="key">The key (dot-separated tokens).</param>
    /// <param name="value">The value.</param>
    /// <param name="options">TTL and expected revision.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public async Task<ulong> PutAsync(string key, ReadOnlyMemory<byte> value, KvPutOptions? options = null, CancellationToken cancellationToken = default)
    {
        ulong? ttl = options?.Ttl is { } t ? Math.Max(1, Durations.Millis(t, "ttl")) : null;
        var req = new Request.KvPut(Bucket, key, value, options?.ExpectedRevision, ttl);
        return (await CallAsync<Response.PublishOk>(req, cancellationToken).ConfigureAwait(false)).Offset;
    }

    /// <summary>Set <paramref name="key"/> to UTF-8 text; returns the new revision.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value text.</param>
    /// <param name="options">TTL and expected revision.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public Task<ulong> PutAsync(string key, string value, KvPutOptions? options = null, CancellationToken cancellationToken = default) =>
        PutAsync(key, Encoding.UTF8.GetBytes(value), options, cancellationToken);

    /// <summary>Set <paramref name="key"/> only if it doesn't exist (or was deleted); <see cref="ExspeedServerException"/> 409 otherwise.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="ttl">Expire this value this long after the put.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public Task<ulong> CreateKeyAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        PutAsync(key, value, new KvPutOptions { Ttl = ttl, ExpectedRevision = 0 }, cancellationToken);

    /// <summary>Set <paramref name="key"/> to UTF-8 text only if it doesn't exist (or was deleted).</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value text.</param>
    /// <param name="ttl">Expire this value this long after the put.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public Task<ulong> CreateKeyAsync(string key, string value, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        CreateKeyAsync(key, Encoding.UTF8.GetBytes(value), ttl, cancellationToken);

    /// <summary>
    /// Set <paramref name="key"/> only if it is at <paramref name="revision"/> (compare-and-set);
    /// <see cref="ExspeedServerException"/> 409 otherwise, with <c>detail.current_revision</c>.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="revision">The revision the key must be at.</param>
    /// <param name="ttl">Expire this value this long after the put.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public Task<ulong> UpdateAsync(string key, ReadOnlyMemory<byte> value, ulong revision, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        PutAsync(key, value, new KvPutOptions { Ttl = ttl, ExpectedRevision = revision }, cancellationToken);

    /// <summary>Compare-and-set with a UTF-8 text value (see <see cref="UpdateAsync(string, ReadOnlyMemory{byte}, ulong, TimeSpan?, CancellationToken)"/>).</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value text.</param>
    /// <param name="revision">The revision the key must be at.</param>
    /// <param name="ttl">Expire this value this long after the put.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The new revision.</returns>
    public Task<ulong> UpdateAsync(string key, string value, ulong revision, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        UpdateAsync(key, Encoding.UTF8.GetBytes(value), revision, ttl, cancellationToken);

    /// <summary>Delete <paramref name="key"/> (its history stays until it ages out); returns the tombstone's revision.</summary>
    /// <param name="key">The key.</param>
    /// <param name="options">Expected revision.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The tombstone's revision.</returns>
    public async Task<ulong> DeleteAsync(string key, KvDeleteOptions? options = null, CancellationToken cancellationToken = default) =>
        (await CallAsync<Response.PublishOk>(new Request.KvDelete(Bucket, key, false, options?.ExpectedRevision), cancellationToken)
            .ConfigureAwait(false)).Offset;

    /// <summary>Delete <paramref name="key"/> and hide its older values; returns the tombstone's revision.</summary>
    /// <param name="key">The key.</param>
    /// <param name="options">Expected revision.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The tombstone's revision.</returns>
    public async Task<ulong> PurgeAsync(string key, KvDeleteOptions? options = null, CancellationToken cancellationToken = default) =>
        (await CallAsync<Response.PublishOk>(new Request.KvDelete(Bucket, key, true, options?.ExpectedRevision), cancellationToken)
            .ConfigureAwait(false)).Offset;

    /// <summary>Keys that have a value, matching <paramref name="filter"/> (NATS-style; empty = all), sorted.</summary>
    /// <param name="filter">A subject filter, e.g. <c>app.*</c>.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The keys.</returns>
    public async Task<IReadOnlyList<string>> KeysAsync(string filter = "", CancellationToken cancellationToken = default)
    {
        var req = new Request.KvKeys(Bucket, filter);
        var json = await CallAsync<Response.Json>(req, cancellationToken).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(json.Body);
            return doc.RootElement.EnumerateArray().Select(e => e.GetString() ?? "").ToList();
        }
        catch (Exception e) when (e is JsonException || e is InvalidOperationException)
        {
            throw new ExspeedProtocolException($"bad JSON in reply to KvKeys: {e.Message}");
        }
    }

    /// <summary>Kept revisions of <paramref name="key"/>, oldest first (deletes included).</summary>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The revisions.</returns>
    public async Task<IReadOnlyList<KvEntry>> HistoryAsync(string key, CancellationToken cancellationToken = default)
    {
        var m = await CallAsync<Response.Messages>(new Request.KvHistory(Bucket, key), cancellationToken).ConfigureAwait(false);
        return m.Records.Select(r => new KvEntry(r)).ToList();
    }

    /// <summary>
    /// Watch keys matching <paramref name="filter"/> (empty = all): first the current value of every matching key
    /// (deleted keys left out), then every change as it happens, deletes included.
    /// </summary>
    /// <param name="filter">A subject filter.</param>
    /// <returns>The watch.</returns>
    public KvWatch Watch(string filter = "") => new(_host, Stream, filter);
}

/// <summary>
/// Changes to a bucket's keys, from <see cref="KvBucket.Watch"/>. Enumerate it with <c>await foreach</c>, or call
/// <see cref="NextAsync"/>. It reads the bucket's stream with stateless long-poll reads, so it holds no state on
/// the server; a failed read (for example <see cref="ExspeedConnectionException"/> when the connection drops)
/// fails <see cref="NextAsync"/>.
/// </summary>
public sealed class KvWatch : IAsyncEnumerable<KvEntry>, IAsyncDisposable
{
    /// <summary>Long-poll wait of each follow-up read.</summary>
    internal static readonly TimeSpan WaitPerRead = TimeSpan.FromSeconds(10);
    private const uint Batch = 1000;

    private readonly IKvHost _host;
    private readonly string _stream;
    private readonly string _filter;
    private readonly object _gate = new();
    private readonly Queue<KvEntry> _pending = new();
    private ulong _from;
    private bool _snapshotDone;
    private Task? _fetching;
    private volatile bool _stopped;

    internal KvWatch(IKvHost host, string stream, string filter)
    {
        _host = host;
        _stream = stream;
        _filter = filter;
    }

    /// <summary>True after <see cref="Stop"/>.</summary>
    public bool IsClosed => _stopped;

    /// <summary>
    /// The next entry, waiting for changes as long as it takes; with <paramref name="timeout"/>, <c>null</c> when
    /// nothing arrived in time. <c>null</c> after <see cref="Stop"/>.
    /// </summary>
    /// <param name="timeout">How long to wait; <c>null</c> = indefinitely.</param>
    /// <param name="cancellationToken">Cancels the wait (a read in flight keeps running and its records are kept).</param>
    /// <returns>The entry, or <c>null</c>.</returns>
    public async Task<KvEntry?> NextAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var deadline = timeout is { } t ? DateTime.UtcNow + t : (DateTime?)null;
        while (true)
        {
            Task fill;
            lock (_gate)
            {
                if (_stopped)
                {
                    return null;
                }
                if (_pending.Count > 0)
                {
                    return _pending.Dequeue();
                }
                fill = Fill();
            }
            if (deadline is null)
            {
                await fill.WaitAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }
            var left = deadline.Value - DateTime.UtcNow;
            if (left <= TimeSpan.Zero)
            {
                return null;
            }
            try
            {
                await fill.WaitAsync(left, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                return null;
            }
        }
    }

    /// <summary>Stop watching: <see cref="NextAsync"/> returns <c>null</c> from now on.</summary>
    public void Stop()
    {
        lock (_gate)
        {
            _stopped = true;
            _pending.Clear();
        }
    }

    /// <summary>Same as <see cref="Stop"/>.</summary>
    /// <returns>A completed task.</returns>
    public ValueTask DisposeAsync()
    {
        Stop();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<KvEntry> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        try
        {
            while (true)
            {
                var e = await NextAsync(null, cancellationToken).ConfigureAwait(false);
                if (e is null)
                {
                    yield break;
                }
                yield return e;
            }
        }
        finally
        {
            Stop();
        }
    }

    /// <summary>One read at a time; a <see cref="NextAsync"/> that timed out leaves it running, and its records are kept.</summary>
    private Task Fill()
    {
        if (_fetching is { } running)
        {
            return running;
        }
        var task = Task.Run(() => _snapshotDone ? FollowAsync() : LoadSnapshotAsync());
        _fetching = task;
        task.ContinueWith(
            t =>
            {
                _ = t.Exception; // observed: surfaced through the NextAsync that awaits it
                lock (_gate)
                {
                    if (ReferenceEquals(_fetching, task))
                    {
                        _fetching = null;
                    }
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return task;
    }

    /// <summary>
    /// Read up to the high watermark seen by the first read, keep the last record per key, and queue the live
    /// keys sorted by revision.
    /// </summary>
    private async Task LoadSnapshotAsync()
    {
        var latest = new Dictionary<string, KvEntry>();
        ulong? end = null;
        while (true)
        {
            var r = await _host.ReadRawAsync(_stream, new ReadOptions { From = _from, MaxRecords = Batch, Filter = _filter }, CancellationToken.None)
                .ConfigureAwait(false);
            end ??= r.HighWatermark;
            foreach (var rec in r.Records)
            {
                var e = new KvEntry(rec);
                latest[e.Key] = e;
            }
            bool progressed = r.NextOffset > _from;
            _from = Math.Max(_from, r.NextOffset);
            if (_from >= end || !progressed)
            {
                break;
            }
        }
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }
            foreach (var e in latest.Values.Where(e => e.Op == KvOp.Put).OrderBy(e => e.Revision))
            {
                _pending.Enqueue(e);
            }
            _snapshotDone = true;
        }
    }

    private async Task FollowAsync()
    {
        var r = await _host.ReadRawAsync(
            _stream,
            new ReadOptions { From = _from, MaxRecords = Batch, Wait = WaitPerRead, Filter = _filter },
            CancellationToken.None).ConfigureAwait(false);
        lock (_gate)
        {
            _from = Math.Max(_from, r.NextOffset);
            if (_stopped)
            {
                return;
            }
            foreach (var rec in r.Records)
            {
                _pending.Enqueue(new KvEntry(rec));
            }
        }
    }
}
