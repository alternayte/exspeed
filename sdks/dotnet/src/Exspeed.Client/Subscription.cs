using Exspeed.Protocol;

namespace Exspeed;

/// <summary>What a subscription needs from its client.</summary>
internal interface ISubscriptionHost : IMessageSettler
{
    void Forget(Subscription sub);
}

/// <summary>
/// A push subscription to a consumer, from <see cref="ExspeedClient.SubscribeAsync"/>. Enumerate it with
/// <c>await foreach</c>; it ends when the server ends it (see <see cref="EndReason"/>), on
/// <see cref="UnsubscribeAsync"/>, or when the client closes. Leaving an <c>await foreach</c> loop early
/// (<c>break</c>, an exception) unsubscribes.
/// </summary>
/// <remarks>
/// Credit flow: the server pushes at most <see cref="Window"/> records ahead of your code. Each message you take
/// returns one credit (sent in batches of half the window), so a slow consumer slows delivery down instead of
/// buffering without bound.
/// </remarks>
public sealed class Subscription : IAsyncEnumerable<Message>, IAsyncDisposable, ISubscriptionSink
{
    private readonly ISubscriptionHost _host;
    private readonly PushQueue<WireRecord> _queue;
    private readonly object _gate = new();
    private Connection? _conn;
    private uint _subId;
    private uint _consumed;
    private EndReason? _endReason;

    internal Subscription(ISubscriptionHost host, string consumer, uint window)
    {
        _host = host;
        Consumer = consumer;
        Window = window;
        _queue = new PushQueue<WireRecord>(_ => ReturnCredit());
    }

    /// <summary>The consumer this subscription receives from.</summary>
    public string Consumer { get; }

    /// <summary>The credit window.</summary>
    public uint Window { get; }

    /// <summary>Server-assigned id of the current subscription (changes after a reconnect).</summary>
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

    /// <summary>Records received but not yet taken.</summary>
    public int Buffered => _queue.Count;

    /// <summary>
    /// The next message, or <c>null</c> once the subscription has ended or, with <paramref name="timeout"/>, when
    /// nothing arrived in time.
    /// </summary>
    /// <param name="timeout">How long to wait; <c>null</c> waits until a message arrives or the subscription ends.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The message, or <c>null</c>.</returns>
    public async Task<Message?> NextAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var r = await _queue.NextAsync(timeout, cancellationToken).ConfigureAwait(false);
        return r is null ? null : new Message(r, Consumer, _host);
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<Message> GetAsyncEnumerator(CancellationToken cancellationToken = default)
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

    /// <summary>
    /// Stop delivery. Records delivered to this subscription and not yet acked (including buffered ones your code
    /// never saw) are redelivered by the server.
    /// </summary>
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
                _consumed = 0;
                return;
            }
        }
        // Unsubscribed while a (re-)subscribe was in flight.
        conn.RemoveSub(subId);
        conn.Send(new Request.Unsubscribe(subId));
    }

    void ISubscriptionSink.OnDeliver(IReadOnlyList<WireRecord> records) => _queue.Enqueue(records);

    void ISubscriptionSink.OnEnded(int code, string message) => End(new EndReason(code, message), true);

    /// <summary>The connection was lost; a re-subscribe will follow. Buffered records are dropped (the server redelivers them).</summary>
    internal void Suspend()
    {
        lock (_gate)
        {
            _conn = null;
            _consumed = 0;
        }
        _queue.Clear();
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
        _host.Forget(this);
    }

    private void ReturnCredit()
    {
        Connection? conn;
        uint credits;
        uint subId;
        lock (_gate)
        {
            conn = _conn;
            if (conn is null)
            {
                return;
            }
            _consumed++;
            if (_consumed < Math.Max(1, Window / 2))
            {
                return;
            }
            credits = _consumed;
            _consumed = 0;
            subId = _subId;
        }
        conn.Send(new Request.Credit(subId, credits));
    }
}
