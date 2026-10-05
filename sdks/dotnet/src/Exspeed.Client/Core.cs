using System.Text;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>What core messages and subscriptions need from the client.</summary>
internal interface ICoreHost
{
    Task PublishCoreAsync(string subject, ReadOnlyMemory<byte> value, CorePublishOptions? options, CancellationToken cancellationToken);

    void ForgetCore(CoreSubscription sub);
}

/// <summary>Options for <see cref="ExspeedClient.PublishCoreAsync(string, ReadOnlyMemory{byte}, CorePublishOptions?, CancellationToken)"/>.</summary>
public sealed record CorePublishOptions
{
    /// <summary>Message headers.</summary>
    public IReadOnlyList<KeyValuePair<string, string>>? Headers { get; init; }

    /// <summary>
    /// Ask receivers to answer on this subject. The publish then fails with <see cref="ExspeedServerException"/>
    /// 404 when nobody received it. <see cref="ExspeedClient.RequestAsync(string, ReadOnlyMemory{byte}, CoreRequestOptions?, CancellationToken)"/>
    /// sets this for you.
    /// </summary>
    public string? ReplyTo { get; init; }
}

/// <summary>Options for <see cref="ExspeedClient.SubscribeCoreAsync"/>.</summary>
public sealed record CoreSubscribeOptions
{
    /// <summary>Queue group: each message goes to one member of the group.</summary>
    public string? Queue { get; init; }
}

/// <summary>Options for <see cref="ExspeedClient.RequestAsync(string, ReadOnlyMemory{byte}, CoreRequestOptions?, CancellationToken)"/>.</summary>
public sealed record CoreRequestOptions
{
    /// <summary>How long to wait for the first response. Default: the client's request timeout.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Request headers.</summary>
    public IReadOnlyList<KeyValuePair<string, string>>? Headers { get; init; }
}

/// <summary>A core (non-persistent) message: from a core subscription, or the response to a request.</summary>
public sealed class CoreMessage
{
    private readonly ICoreHost _host;

    internal CoreMessage(Response.CoreMsg m, ICoreHost host)
    {
        Subject = m.Subject;
        ReplyTo = m.ReplyTo;
        Headers = m.Headers;
        Value = m.Value;
        _host = host;
    }

    /// <summary>The subject it was published to.</summary>
    public string Subject { get; }

    /// <summary>Set on a request: answer it with <see cref="RespondAsync(ReadOnlyMemory{byte}, IReadOnlyList{KeyValuePair{string, string}}?, CancellationToken)"/>.</summary>
    public string? ReplyTo { get; }

    /// <summary>Headers in wire order; a name can repeat. See <see cref="Header"/>.</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Headers { get; }

    /// <summary>The payload.</summary>
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>The value as UTF-8 text.</summary>
    /// <returns>The text.</returns>
    public string Text() => Encoding.UTF8.GetString(Value.Span);

    /// <summary>The value parsed as JSON.</summary>
    /// <typeparam name="T">The type to deserialize.</typeparam>
    /// <param name="options">Serializer options.</param>
    /// <returns>The deserialized value.</returns>
    public T? Json<T>(JsonSerializerOptions? options = null) => JsonSerializer.Deserialize<T>(Value.Span, options);

    /// <summary>The first header named <paramref name="name"/>, or <c>null</c>.</summary>
    /// <param name="name">The header name.</param>
    /// <returns>Its value.</returns>
    public string? Header(string name)
    {
        foreach (var (k, v) in Headers)
        {
            if (k == name)
            {
                return v;
            }
        }
        return null;
    }

    /// <summary>Answer a request: publish <paramref name="value"/> to its <see cref="ReplyTo"/> subject.</summary>
    /// <param name="value">The response payload.</param>
    /// <param name="headers">Response headers.</param>
    /// <param name="cancellationToken">Cancels waiting for the server's reply.</param>
    /// <returns>A task that completes once the server accepted the response.</returns>
    public Task RespondAsync(ReadOnlyMemory<byte> value, IReadOnlyList<KeyValuePair<string, string>>? headers = null, CancellationToken cancellationToken = default)
    {
        if (ReplyTo is null)
        {
            return Task.FromException(new ExspeedException("message has no replyTo"));
        }
        return _host.PublishCoreAsync(ReplyTo, value, new CorePublishOptions { Headers = headers }, cancellationToken);
    }

    /// <summary>Answer a request with UTF-8 text.</summary>
    /// <param name="value">The response text.</param>
    /// <param name="headers">Response headers.</param>
    /// <param name="cancellationToken">Cancels waiting for the server's reply.</param>
    /// <returns>A task that completes once the server accepted the response.</returns>
    public Task RespondAsync(string value, IReadOnlyList<KeyValuePair<string, string>>? headers = null, CancellationToken cancellationToken = default) =>
        RespondAsync(Encoding.UTF8.GetBytes(value), headers, cancellationToken);
}

/// <summary>
/// A core-message subscription, from <see cref="ExspeedClient.SubscribeCoreAsync"/>. Enumerate it with
/// <c>await foreach</c>; it ends on <see cref="UnsubscribeAsync"/> (or leaving the loop early), when the server
/// ends it (503 when leadership moves), or when the client closes.
/// </summary>
/// <remarks>
/// Core messages are not stored: a subscription receives what is published while it is live. After a reconnect
/// the client subscribes again with the same subject and queue group; messages published in the gap are missed.
/// </remarks>
public sealed class CoreSubscription : IAsyncEnumerable<CoreMessage>, IAsyncDisposable, ISubscriptionSink
{
    private readonly ICoreHost _host;
    private readonly PushQueue<CoreMessage> _queue = new();
    private readonly object _gate = new();
    private Connection? _conn;
    private uint _subId;
    private EndReason? _endReason;

    internal CoreSubscription(ICoreHost host, string subject, string? queue)
    {
        _host = host;
        Subject = subject;
        Queue = queue;
    }

    /// <summary>The subject filter.</summary>
    public string Subject { get; }

    /// <summary>The queue group, if any.</summary>
    public string? Queue { get; }

    /// <summary>Server-assigned id of the current subscription (high bit set; changes after a reconnect).</summary>
    public uint Id
    {
        get
        {
            lock (_gate)
            {
                return _subId;
            }
        }
    }

    /// <summary>Why the subscription ended, or <c>null</c> while it is active.</summary>
    public EndReason? EndReason
    {
        get
        {
            lock (_gate)
            {
                return _endReason;
            }
        }
    }

    /// <summary>True once the subscription has ended.</summary>
    public bool IsClosed => EndReason is not null;

    /// <summary>Messages received but not yet taken.</summary>
    public int Buffered => _queue.Count;

    /// <summary>
    /// The next message, or <c>null</c> once the subscription has ended or, with <paramref name="timeout"/>, when
    /// nothing arrived in time.
    /// </summary>
    /// <param name="timeout">How long to wait; <c>null</c> waits until a message arrives or the subscription ends.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The message, or <c>null</c>.</returns>
    public Task<CoreMessage?> NextAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        _queue.NextAsync(timeout, cancellationToken);

    /// <inheritdoc />
    public async IAsyncEnumerator<CoreMessage> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        try
        {
            while (true)
            {
                var m = await NextAsync(null, cancellationToken).ConfigureAwait(false);
                if (m is null)
                {
                    yield break;
                }
                yield return m;
            }
        }
        finally
        {
            await UnsubscribeAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Stop delivery. Buffered messages are dropped.</summary>
    /// <param name="cancellationToken">Cancels waiting for the server's reply.</param>
    /// <returns>A task that completes when the server confirmed (or the connection is gone).</returns>
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        Connection? conn;
        uint subId;
        lock (_gate)
        {
            if (_endReason is not null)
            {
                return;
            }
            conn = _conn;
            subId = _subId;
        }
        End(new EndReason(0, "unsubscribed"), false);
        if (conn is { IsClosed: false })
        {
            conn.RemoveSub(subId);
            try
            {
                await conn.RequestAsync(new Request.Unsubscribe(subId), null, null, cancellationToken).ConfigureAwait(false);
            }
            catch (ExspeedException)
            {
                // The subscription is gone either way.
            }
        }
    }

    /// <summary>Same as <see cref="UnsubscribeAsync"/>.</summary>
    /// <returns>A task that completes when unsubscribed.</returns>
    public ValueTask DisposeAsync() => new(UnsubscribeAsync());

    void ISubscriptionSink.OnSubscribed(Connection conn, uint subId)
    {
        lock (_gate)
        {
            if (_endReason is null)
            {
                _conn = conn;
                _subId = subId;
                return;
            }
        }
        conn.RemoveSub(subId);
        conn.Send(new Request.Unsubscribe(subId));
    }

    void ISubscriptionSink.OnCoreMsg(Response.CoreMsg msg) => _queue.Enqueue(new CoreMessage(msg, _host));

    void ISubscriptionSink.OnEnded(int code, string message) => End(new EndReason(code, message), true);

    /// <summary>The connection was lost; a re-subscribe will follow. Buffered messages are kept.</summary>
    internal void Suspend()
    {
        lock (_gate)
        {
            _conn = null;
        }
    }

    internal void End(EndReason reason, bool keepBuffered)
    {
        lock (_gate)
        {
            if (_endReason is not null)
            {
                return;
            }
            _endReason = reason;
            _conn = null;
        }
        _queue.End(keepBuffered);
        _host.ForgetCore(this);
    }
}
