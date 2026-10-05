using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

public sealed partial class ExspeedClient
{
    // ---- basics ---------------------------------------------------------------

    /// <summary>Round trip to the server.</summary>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The round-trip time.</returns>
    public async Task<TimeSpan> PingAsync(CancellationToken cancellationToken = default)
    {
        long start = Stopwatch.GetTimestamp();
        await CallAsync<Response.Pong>(new Request.Ping(), cancellationToken).ConfigureAwait(false);
        return Stopwatch.GetElapsedTime(start);
    }

    /// <summary>Node id, leadership and server version.</summary>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The metadata.</returns>
    public async Task<ServerMetadata> MetadataAsync(CancellationToken cancellationToken = default)
    {
        var e = await JsonAsync(new Request.Metadata(), cancellationToken).ConfigureAwait(false);
        return new ServerMetadata(
            WireJson.Str(e, "node_id") ?? "",
            WireJson.Bool(e, "is_leader"),
            WireJson.Str(e, "leader"),
            WireJson.Str(e, "server_version") ?? "");
    }

    // ---- streams --------------------------------------------------------------

    /// <summary>
    /// Create a stream. Idempotent when it exists with the same settings; <see cref="ExspeedServerException"/> 409
    /// when the settings differ.
    /// </summary>
    /// <param name="spec">The stream's settings.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when the stream exists.</returns>
    public Task CreateStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.CreateStream(spec.ToWire()), cancellationToken);

    /// <summary>Create a stream with the server's default settings.</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when the stream exists.</returns>
    public Task CreateStreamAsync(string name, CancellationToken cancellationToken = default) =>
        CreateStreamAsync(new StreamSpec(name), cancellationToken);

    /// <summary>Replace a stream's settings (unset fields reset to the server defaults).</summary>
    /// <param name="spec">The new settings.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when updated.</returns>
    public Task UpdateStreamAsync(StreamSpec spec, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.UpdateStream(spec.ToWire()), cancellationToken);

    /// <summary>Delete a stream. Fails with 409 while consumers exist (<c>detail.consumers</c>).</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when deleted.</returns>
    public Task DeleteStreamAsync(string name, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.DeleteStream(name), cancellationToken);

    /// <summary>A stream's offsets and settings.</summary>
    /// <param name="name">The stream name.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The stream info.</returns>
    public async Task<StreamInfo> StreamInfoAsync(string name, CancellationToken cancellationToken = default) =>
        StreamInfo.Parse(await JsonAsync(new Request.StreamInfo(name), cancellationToken).ConfigureAwait(false));

    /// <summary>The streams this credential can see.</summary>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The streams, sorted by name.</returns>
    public async Task<IReadOnlyList<StreamInfo>> ListStreamsAsync(CancellationToken cancellationToken = default)
    {
        var e = await JsonAsync(new Request.ListStreams(), cancellationToken).ConfigureAwait(false);
        return e.ValueKind == JsonValueKind.Array ? e.EnumerateArray().Select(StreamInfo.Parse).ToList() : new List<StreamInfo>();
    }

    // ---- publishing -----------------------------------------------------------

    /// <summary>Publish one record and wait for its offset.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="record">The record.</param>
    /// <param name="cancellationToken">Stops waiting for the reply (the record may still be written).</param>
    /// <returns>The record's offset and duplicate flag.</returns>
    public async Task<PublishResult> PublishAsync(string stream, PublishRecord record, CancellationToken cancellationToken = default)
    {
        var r = await CallAsync<Response.PublishOk>(new Request.Publish(stream, record.ToWire()), cancellationToken).ConfigureAwait(false);
        return new PublishResult(r.Offset, r.Duplicate);
    }

    /// <summary>Publish a UTF-8 text record and wait for its offset.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="subject">The record's subject.</param>
    /// <param name="value">The payload text.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    /// <returns>The record's offset and duplicate flag.</returns>
    public Task<PublishResult> PublishAsync(string stream, string subject, string value, CancellationToken cancellationToken = default) =>
        PublishAsync(stream, new PublishRecord(subject, value), cancellationToken);

    /// <summary>Publish several records in one request; one result per record, in order.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="records">The records.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    /// <returns>One result per record.</returns>
    public async Task<IReadOnlyList<PublishResult>> PublishBatchAsync(string stream, IEnumerable<PublishRecord> records, CancellationToken cancellationToken = default)
    {
        var wire = records.Select(r => r.ToWire()).ToList();
        if (wire.Count == 0)
        {
            return Array.Empty<PublishResult>();
        }
        var r = await CallAsync<Response.PublishBatchOk>(new Request.PublishBatch(stream, wire), cancellationToken).ConfigureAwait(false);
        return r.Results.Select(x => new PublishResult(x.Offset, x.Duplicate)).ToList();
    }

    /// <summary>A coalescing, order-preserving publisher on this client; see <see cref="Publisher"/>.</summary>
    /// <param name="options">Batching options.</param>
    /// <returns>The publisher.</returns>
    public Publisher CreatePublisher(PublisherOptions? options = null) => new(_hosts, options);

    // ---- stateless reads ------------------------------------------------------

    /// <summary>
    /// Read records without a consumer. With <see cref="ReadOptions.Wait"/>, waits for new records when caught up.
    /// Continue from <see cref="ReadResult.NextOffset"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="options">Where to start, how much, a subject filter and a long-poll wait.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The records and where to continue.</returns>
    public async Task<ReadResult> ReadAsync(string stream, ReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var r = await ReadRawAsync(stream, options ?? new ReadOptions(), cancellationToken).ConfigureAwait(false);
        return new ReadResult(r.Records.Select(w => new StreamRecord(w)).ToList(), r.NextOffset, r.HighWatermark);
    }

    private Task<Response.ReadResult> ReadRawAsync(string stream, ReadOptions o, CancellationToken cancellationToken)
    {
        uint waitMs = Durations.Millis32(o.Wait, "wait");
        var req = new Request.Read(stream, o.From, o.MaxRecords, o.MaxBytes, waitMs, o.Filter ?? "");
        return CallAsync<Response.ReadResult>(req, _connOpts.RequestTimeout + o.Wait, null, cancellationToken);
    }

    // ---- SQL ------------------------------------------------------------------

    /// <summary>Run a bounded ExQL query. Needs a global-admin credential when auth is on.</summary>
    /// <param name="sql">The query.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>Columns and rows.</returns>
    public async Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        var e = await JsonAsync(new Request.Query(sql), cancellationToken).ConfigureAwait(false);
        var columns = new List<string>();
        if (e.TryGetProperty("columns", out var cols) && cols.ValueKind == JsonValueKind.Array)
        {
            columns.AddRange(cols.EnumerateArray().Select(c => c.GetString() ?? ""));
        }
        var rows = new List<IReadOnlyList<JsonElement>>();
        if (e.TryGetProperty("rows", out var rs) && rs.ValueKind == JsonValueKind.Array)
        {
            foreach (var row in rs.EnumerateArray())
            {
                rows.Add(row.ValueKind == JsonValueKind.Array ? row.EnumerateArray().Select(x => x.Clone()).ToList() : new List<JsonElement>());
            }
        }
        double ms = e.TryGetProperty("execution_time_ms", out var t) && t.ValueKind == JsonValueKind.Number ? t.GetDouble() : 0;
        return new QueryResult
        {
            Columns = columns,
            Rows = rows,
            RowCount = WireJson.U64(e, "row_count"),
            ExecutionTimeMs = ms,
            Truncated = WireJson.Bool(e, "truncated"),
        };
    }

    // ---- consumers ------------------------------------------------------------

    /// <summary>
    /// Create a consumer. Idempotent for an identical spec; 409 when a consumer of that name exists with a
    /// different spec. Ephemeral consumers are re-created after a reconnect.
    /// </summary>
    /// <param name="spec">The consumer's spec.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The consumer's info.</returns>
    public async Task<ConsumerInfo> CreateConsumerAsync(ConsumerSpec spec, CancellationToken cancellationToken = default)
    {
        var e = await JsonAsync(new Request.CreateConsumer(spec.ToWireJson()), cancellationToken).ConfigureAwait(false);
        if (spec.Ephemeral == true)
        {
            lock (_stateGate)
            {
                _ephemeral[spec.Name] = spec;
            }
        }
        return ConsumerInfo.Parse(e);
    }

    /// <summary>Delete a consumer.</summary>
    /// <param name="name">The consumer name.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when deleted.</returns>
    public async Task DeleteConsumerAsync(string name, CancellationToken cancellationToken = default)
    {
        await CallAsync<Response.Ok>(new Request.DeleteConsumer(name), cancellationToken).ConfigureAwait(false);
        lock (_stateGate)
        {
            _ephemeral.Remove(name);
        }
    }

    /// <summary>A consumer's spec, position and counters.</summary>
    /// <param name="name">The consumer name.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The consumer's info.</returns>
    public async Task<ConsumerInfo> ConsumerInfoAsync(string name, CancellationToken cancellationToken = default) =>
        ConsumerInfo.Parse(await JsonAsync(new Request.ConsumerInfo(name), cancellationToken).ConfigureAwait(false));

    /// <summary>Consumers this credential can see, optionally only those on <paramref name="stream"/>.</summary>
    /// <param name="stream">Only consumers of this stream; <c>null</c> = all.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The consumers.</returns>
    public async Task<IReadOnlyList<ConsumerInfo>> ListConsumersAsync(string? stream = null, CancellationToken cancellationToken = default)
    {
        var e = await JsonAsync(new Request.ListConsumers(stream), cancellationToken).ConfigureAwait(false);
        return e.ValueKind == JsonValueKind.Array ? e.EnumerateArray().Select(ConsumerInfo.Parse).ToList() : new List<ConsumerInfo>();
    }

    /// <summary>Move a consumer's cursor.</summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="target">Where to: <see cref="SeekTarget.Earliest"/>, <see cref="SeekTarget.Latest"/>, an offset or a time.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when moved.</returns>
    public Task SeekAsync(string consumer, SeekTarget target, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.SeekConsumer(consumer, target.Kind, target.Value), cancellationToken);

    /// <summary>
    /// Start push delivery from a consumer. Any number of subscriptions (on any connection, in any process) can
    /// share one consumer; each record goes to one of them.
    /// </summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="options">The credit window.</param>
    /// <param name="cancellationToken">Cancels subscribing.</param>
    /// <returns>The subscription.</returns>
    public async Task<Subscription> SubscribeAsync(string consumer, SubscribeOptions? options = null, CancellationToken cancellationToken = default)
    {
        uint window = Math.Max(1, (options ?? new SubscribeOptions()).Window);
        var sub = new Subscription(_hosts, consumer, window);
        // The connection binds `sub` to its id as soon as SubscribeOk arrives, before any Deliver behind it.
        await CallAsync<Response.SubscribeOk>(new Request.Subscribe(consumer, window), null, sub, cancellationToken).ConfigureAwait(false);
        lock (_stateGate)
        {
            _subs.Add(sub);
        }
        return sub;
    }

    /// <summary>
    /// Fetch up to <see cref="PullOptions.MaxMessages"/>, waiting up to <see cref="PullOptions.Expires"/> for at least
    /// one. Returns an empty list on timeout.
    /// </summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="options">Batch size and expiry.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The messages.</returns>
    public async Task<IReadOnlyList<Message>> PullAsync(string consumer, PullOptions? options = null, CancellationToken cancellationToken = default)
    {
        var o = options ?? new PullOptions();
        var req = new Request.Pull(consumer, o.MaxMessages, o.MaxBytes, Durations.Millis32(o.Expires, "expires"));
        var r = await CallAsync<Response.Messages>(req, _connOpts.RequestTimeout + o.Expires, null, cancellationToken).ConfigureAwait(false);
        return r.Records.Select(w => new Message(w, consumer, _hosts)).ToList();
    }

    /// <summary>Acknowledge records and wait for the server to confirm.</summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="offsets">The offsets to ack.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when confirmed.</returns>
    public Task AckAsync(string consumer, IEnumerable<ulong> offsets, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.Ack(consumer, offsets.ToList()), cancellationToken);

    /// <summary>Redeliver after <paramref name="delay"/> (<c>null</c> or zero = the consumer's backoff).</summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="offset">The record's offset.</param>
    /// <param name="delay">How long to wait before redelivering.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when confirmed.</returns>
    public Task NackAsync(string consumer, ulong offset, TimeSpan? delay = null, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.Nack(consumer, offset, Durations.Millis32(delay ?? TimeSpan.Zero, "delay")), cancellationToken);

    /// <summary>Dead-letter now (to the consumer's DLQ stream, if set).</summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="offset">The record's offset.</param>
    /// <param name="reason">Free text for the <c>exspeed-dlq-reason</c> header.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when confirmed.</returns>
    public Task TermAsync(string consumer, ulong offset, string reason = "", CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.Term(consumer, offset, reason), cancellationToken);

    /// <summary>Reset the ack deadlines of records still being worked on.</summary>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="offsets">The offsets.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes when confirmed.</returns>
    public Task InProgressAsync(string consumer, IEnumerable<ulong> offsets, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(new Request.InProgress(consumer, offsets.ToList()), cancellationToken);

    // ---- core messaging (non-persistent) ---------------------------------------

    /// <summary>
    /// Publish a core message to the core subscriptions live now. Nothing is stored and delivery is at most once.
    /// Completes once the server accepted it.
    /// </summary>
    /// <param name="subject">The subject (no wildcards).</param>
    /// <param name="value">The payload.</param>
    /// <param name="options">Headers and a reply subject.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes once accepted.</returns>
    public Task PublishCoreAsync(string subject, ReadOnlyMemory<byte> value, CorePublishOptions? options = null, CancellationToken cancellationToken = default) =>
        CallAsync<Response.Ok>(
            new Request.CorePublish(subject, options?.ReplyTo, options?.Headers ?? Array.Empty<KeyValuePair<string, string>>(), value),
            cancellationToken);

    /// <summary>Publish a core message with a UTF-8 text payload.</summary>
    /// <param name="subject">The subject.</param>
    /// <param name="value">The payload text.</param>
    /// <param name="options">Headers and a reply subject.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>A task that completes once accepted.</returns>
    public Task PublishCoreAsync(string subject, string value, CorePublishOptions? options = null, CancellationToken cancellationToken = default) =>
        PublishCoreAsync(subject, Encoding.UTF8.GetBytes(value), options, cancellationToken);

    /// <summary>
    /// Receive core messages on subjects matching <paramref name="subject"/> (a filter such as <c>orders.*</c>). With
    /// a queue group, each message goes to one member of the group.
    /// </summary>
    /// <param name="subject">The subject filter.</param>
    /// <param name="options">The queue group.</param>
    /// <param name="cancellationToken">Cancels subscribing.</param>
    /// <returns>The subscription.</returns>
    public async Task<CoreSubscription> SubscribeCoreAsync(string subject, CoreSubscribeOptions? options = null, CancellationToken cancellationToken = default)
    {
        var sub = new CoreSubscription(_hosts, subject, options?.Queue);
        await CallAsync<Response.SubscribeOk>(new Request.CoreSubscribe(subject, sub.Queue), null, sub, cancellationToken).ConfigureAwait(false);
        lock (_stateGate)
        {
            _coreSubs.Add(sub);
        }
        return sub;
    }

    /// <summary>
    /// Send a request (a core message with a reply subject) and return the first response. Fails with
    /// <see cref="ExspeedServerException"/> 404 at once when nobody is subscribed to <paramref name="subject"/>, and
    /// with <see cref="ExspeedTimeoutException"/> after the timeout.
    /// </summary>
    /// <remarks>
    /// All requests on a connection share one inbox subscription (<c>_INBOX.&lt;random&gt;.*</c>), set up by the
    /// first request.
    /// </remarks>
    /// <param name="subject">The request subject.</param>
    /// <param name="value">The request payload.</param>
    /// <param name="options">Timeout and headers.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The response.</returns>
    public async Task<CoreMessage> RequestAsync(string subject, ReadOnlyMemory<byte> value, CoreRequestOptions? options = null, CancellationToken cancellationToken = default)
    {
        var timeout = options?.Timeout ?? _connOpts.RequestTimeout;
        long start = Stopwatch.GetTimestamp();
        var prefix = await EnsureInboxAsync(cancellationToken).ConfigureAwait(false);
        string token = Interlocked.Increment(ref _nextReply).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var reply = new TaskCompletionSource<CoreMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_replies)
        {
            _replies[token] = reply;
        }
        try
        {
            var req = new Request.CorePublish(
                subject,
                $"{prefix}.{token}",
                options?.Headers ?? Array.Empty<KeyValuePair<string, string>>(),
                value);
            await CallAsync<Response.Ok>(req, timeout, null, cancellationToken).ConfigureAwait(false);
            var left = timeout - Stopwatch.GetElapsedTime(start);
            return await reply.Task.WaitAsync(left > TimeSpan.Zero ? left : TimeSpan.Zero, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new ExspeedTimeoutException($"request to {subject} timed out after {(long)timeout.TotalMilliseconds} ms");
        }
        finally
        {
            lock (_replies)
            {
                _replies.Remove(token);
            }
        }
    }

    /// <summary>Send a request with a UTF-8 text payload (see <see cref="RequestAsync(string, ReadOnlyMemory{byte}, CoreRequestOptions?, CancellationToken)"/>).</summary>
    /// <param name="subject">The request subject.</param>
    /// <param name="value">The request text.</param>
    /// <param name="options">Timeout and headers.</param>
    /// <param name="cancellationToken">Cancels the request.</param>
    /// <returns>The response.</returns>
    public Task<CoreMessage> RequestAsync(string subject, string value, CoreRequestOptions? options = null, CancellationToken cancellationToken = default) =>
        RequestAsync(subject, Encoding.UTF8.GetBytes(value), options, cancellationToken);

    // ---- key-value buckets ----------------------------------------------------

    /// <summary>A handle to the key-value bucket <paramref name="bucket"/> (create it with <see cref="KvBucket.CreateAsync"/>).</summary>
    /// <param name="bucket">The bucket name (<c>[A-Za-z0-9_-]</c>).</param>
    /// <returns>The bucket handle.</returns>
    public KvBucket Kv(string bucket) => new(_hosts, bucket);
}
