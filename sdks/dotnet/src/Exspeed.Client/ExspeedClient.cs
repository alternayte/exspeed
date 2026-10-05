using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>
/// A connection to an Exspeed server (client protocol v2).
/// </summary>
/// <remarks>
/// <para>
/// One client is one TCP/TLS connection. Requests are multiplexed by correlation id, so a slow pull or long-poll
/// read never blocks other calls; share one client across your application. The client is thread-safe.
/// </para>
/// <para>
/// Events (raised on the thread that noticed the change; keep handlers short):
/// <see cref="Disconnected"/> (the connection dropped; reconnecting), <see cref="Reconnected"/> (subscriptions
/// restored), <see cref="Closed"/> (closed for good) and <see cref="AsyncError"/> (a fire-and-forget request such
/// as an ack failed).
/// </para>
/// </remarks>
public sealed partial class ExspeedClient : IAsyncDisposable
{
    private enum State
    {
        Connected,
        Reconnecting,
        Closed,
    }

    /// <summary>Subjects of request-reply inboxes start with this token.</summary>
    private const string InboxPrefix = "_INBOX";

    private readonly ConnectionOptions _connOpts;
    private readonly ReconnectOptions? _reconnect;
    private readonly IReadOnlyList<string> _servers;
    private readonly object _stateGate = new();
    private readonly HashSet<Subscription> _subs = new();
    private readonly HashSet<CoreSubscription> _coreSubs = new();
    private readonly Dictionary<string, ConsumerSpec> _ephemeral = new();
    private readonly Dictionary<string, TaskCompletionSource<CoreMessage>> _replies = new();
    private readonly object _ackGate = new();
    private readonly Dictionary<string, List<ulong>> _pendingAcks = new();
    private readonly CancellationTokenSource _closeCts = new();
    private readonly Hosts _hosts;
    private volatile Connection _conn = null!;
    private volatile State _state = State.Connected;
    private Inbox? _inbox;
    private long _nextReply;
    private bool _ackFlushScheduled;

    private ExspeedClient(ConnectionOptions connOpts, ReconnectOptions? reconnect, IReadOnlyList<string> servers)
    {
        _connOpts = connOpts;
        _reconnect = reconnect;
        _servers = servers;
        _hosts = new Hosts(this);
    }

    /// <summary>The connection dropped; the client is reconnecting. Carries the cause.</summary>
    public event EventHandler<ExspeedErrorEventArgs>? Disconnected;

    /// <summary>The client reconnected; consumer and core subscriptions are restored.</summary>
    public event EventHandler<ExspeedReconnectedEventArgs>? Reconnected;

    /// <summary>
    /// The client is closed for good: by <see cref="CloseAsync"/> (no error), or because the connection dropped
    /// and reconnecting is off or gave up (the cause).
    /// </summary>
    public event EventHandler<ExspeedErrorEventArgs>? Closed;

    /// <summary>
    /// A fire-and-forget request (ack, credit) failed: an <see cref="ExspeedServerException"/> or an
    /// <see cref="ExspeedProtocolException"/> the server sent with correlation id 0.
    /// </summary>
    public event EventHandler<ExspeedErrorEventArgs>? AsyncError;

    /// <summary>Connect and authenticate. The first connection attempt is not retried.</summary>
    /// <param name="options">Connection options; <c>null</c> = <c>127.0.0.1:5933</c> with the defaults.</param>
    /// <param name="cancellationToken">Cancels connecting.</param>
    /// <returns>The connected client.</returns>
    /// <exception cref="ExspeedConnectionException">The server could not be reached, or the TLS handshake failed.</exception>
    /// <exception cref="ExspeedServerException">The server rejected the handshake (401 for a bad or missing token).</exception>
    public static async Task<ExspeedClient> ConnectAsync(ExspeedClientOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new ExspeedClientOptions();
        var connOpts = new ConnectionOptions(
            options.Host,
            options.Port,
            options.Tls,
            options.ClientId,
            options.Token,
            options.RequestTimeout,
            options.Keepalive);
        var client = new ExspeedClient(connOpts, options.Reconnect, options.Servers ?? Array.Empty<string>());
        var conn = await client.OpenLeaderAsync(null, cancellationToken).ConfigureAwait(false);
        client._conn = conn;
        if (conn.IsClosed)
        {
            client.OnConnectionLost(conn, new ExspeedConnectionException("connection closed"));
        }
        return client;
    }

    /// <summary>Handshake info from the current connection.</summary>
    public ServerInfo ServerInfo => _conn.Info;

    /// <summary>True while a connection is up (false while reconnecting or after closing).</summary>
    public bool IsConnected => _state == State.Connected && !_conn.IsClosed;

    /// <summary>
    /// Close the connection. Pending requests fail with <see cref="ExspeedConnectionException"/>, subscriptions end
    /// (code 0), and the server deletes this connection's ephemeral consumers. Acks made before the call are sent
    /// first.
    /// </summary>
    /// <returns>A task that completes when the connection is closed.</returns>
    public async Task CloseAsync()
    {
        List<Subscription> subs;
        List<CoreSubscription> coreSubs;
        lock (_stateGate)
        {
            if (_state == State.Closed)
            {
                return;
            }
            FlushAcks(); // acks made just before CloseAsync still go out
            _state = State.Closed;
            subs = _subs.ToList();
            coreSubs = _coreSubs.ToList();
        }
        foreach (var s in subs)
        {
            s.End(new EndReason(0, "client closed"), false);
        }
        foreach (var s in coreSubs)
        {
            s.End(new EndReason(0, "client closed"), false);
        }
        DropInbox(new ExspeedConnectionException("client closed"));
        _closeCts.Cancel();
        await _conn.CloseAsync().ConfigureAwait(false);
        Raise(Closed, new ExspeedErrorEventArgs(null));
    }

    /// <summary>Same as <see cref="CloseAsync"/>.</summary>
    /// <returns>A task that completes when the connection is closed.</returns>
    public ValueTask DisposeAsync() => new(CloseAsync());

    // ---- plumbing ------------------------------------------------------------

    /// <summary>Send a request on the current connection (queued before this returns).</summary>
    internal Task<Response> RawRequestAsync(Request req, TimeSpan? timeout, ISubscriptionSink? sink, CancellationToken cancellationToken)
    {
        var state = _state;
        if (state == State.Closed)
        {
            return Task.FromException<Response>(new ExspeedConnectionException("client is closed"));
        }
        if (state == State.Reconnecting)
        {
            return Task.FromException<Response>(new ExspeedConnectionException("not connected (reconnecting)"));
        }
        FlushAcks();
        return _conn.RequestAsync(req, timeout, sink, cancellationToken);
    }

    private async Task<T> CallAsync<T>(Request req, TimeSpan? timeout, ISubscriptionSink? sink, CancellationToken cancellationToken)
        where T : Response
    {
        var resp = await RawRequestAsync(req, timeout, sink, cancellationToken).ConfigureAwait(false);
        return resp as T ?? throw new ExspeedProtocolException($"unexpected reply to {req.Name}: {resp.Name}");
    }

    private Task<T> CallAsync<T>(Request req, CancellationToken cancellationToken)
        where T : Response =>
        CallAsync<T>(req, null, null, cancellationToken);

    private async Task<JsonElement> JsonAsync(Request req, CancellationToken cancellationToken)
    {
        var r = await CallAsync<Response.Json>(req, cancellationToken).ConfigureAwait(false);
        try
        {
            using var doc = JsonDocument.Parse(r.Body);
            return doc.RootElement.Clone();
        }
        catch (JsonException e)
        {
            throw new ExspeedProtocolException($"bad JSON in reply to {req.Name}: {e.Message}");
        }
    }

    /// <summary>
    /// Queue a fire-and-forget ack. Acks made close together go out as one <c>Ack</c> frame per consumer, and
    /// always before any later request, so wire order matches call order. Coalescing matters: the server does
    /// work per <c>Ack</c> command.
    /// </summary>
    private void AckNoWait(string consumer, ulong offset)
    {
        if (_state != State.Connected)
        {
            return; // redelivered after the reconnect
        }
        lock (_ackGate)
        {
            if (!_pendingAcks.TryGetValue(consumer, out var list))
            {
                list = new List<ulong>();
                _pendingAcks[consumer] = list;
            }
            list.Add(offset);
            if (!_ackFlushScheduled)
            {
                _ackFlushScheduled = true;
                ThreadPool.UnsafeQueueUserWorkItem(_ => FlushAcks(), null);
            }
        }
    }

    private void FlushAcks()
    {
        lock (_ackGate)
        {
            _ackFlushScheduled = false;
            if (_pendingAcks.Count == 0)
            {
                return;
            }
            var acks = _pendingAcks.ToList();
            _pendingAcks.Clear();
            if (_state != State.Connected)
            {
                return;
            }
            var conn = _conn;
            foreach (var (consumer, offsets) in acks)
            {
                conn.Send(new Request.Ack(consumer, offsets));
            }
        }
    }

    private void Raise<T>(EventHandler<T>? handler, T args)
    {
        if (handler is null)
        {
            return;
        }
        try
        {
            handler(this, args);
        }
        catch (Exception)
        {
            // A failing event handler must not break the client.
        }
    }

    private void OnAsyncError(Exception err) => Raise(AsyncError, new ExspeedErrorEventArgs(err));

    private void OnConnectionLost(Connection lost, Exception err)
    {
        List<Subscription> subs;
        List<CoreSubscription> coreSubs;
        bool reconnect = _reconnect is not null;
        lock (_stateGate)
        {
            if (!ReferenceEquals(_conn, lost) || _state != State.Connected)
            {
                return;
            }
            _state = reconnect ? State.Reconnecting : State.Closed;
            subs = _subs.ToList();
            coreSubs = _coreSubs.ToList();
        }
        // Responses to the inbox can't arrive any more; a request after the reconnect subscribes a new inbox.
        DropInbox(new ExspeedConnectionException($"connection lost: {err.Message}", err));
        if (!reconnect)
        {
            foreach (var s in subs)
            {
                s.End(new EndReason(ErrorCodes.Unavailable, "connection closed"), false);
            }
            foreach (var s in coreSubs)
            {
                s.End(new EndReason(ErrorCodes.Unavailable, "connection closed"), false);
            }
            Raise(Closed, new ExspeedErrorEventArgs(err));
            return;
        }
        lock (_ackGate)
        {
            _pendingAcks.Clear(); // those records will be redelivered
        }
        foreach (var s in subs)
        {
            s.Suspend();
        }
        foreach (var s in coreSubs)
        {
            s.Suspend();
        }
        Raise(Disconnected, new ExspeedErrorEventArgs(err));
        _ = Task.Run(() => ReconnectLoopAsync(_reconnect!, lost.Info.Leader));
    }

    private async Task ReconnectLoopAsync(ReconnectOptions opts, string? leaderHint)
    {
        Exception lastErr = new ExspeedConnectionException("connection lost");
        for (int attempt = 1; attempt <= opts.MaxAttempts; attempt++)
        {
            double baseMs = Math.Min(
                opts.InitialDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt - 1, 30)),
                opts.MaxDelay.TotalMilliseconds);
            double delayMs = baseMs * (0.75 + (Random.Shared.NextDouble() * 0.5));
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(0, delayMs)), _closeCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return; // closed meanwhile
            }
            if (_state != State.Reconnecting)
            {
                return;
            }
            Connection conn;
            try
            {
                conn = await OpenLeaderAsync(leaderHint, _closeCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception err)
            {
                lastErr = err;
                // A rejected credential won't get better by retrying.
                if (err is ExspeedServerException { Code: ErrorCodes.Unauthorized or ErrorCodes.Forbidden })
                {
                    break;
                }
                continue;
            }
            lock (_stateGate)
            {
                if (_state != State.Reconnecting)
                {
                    _ = conn.CloseAsync();
                    return;
                }
                _conn = conn;
                _state = State.Connected;
            }
            if (conn.IsClosed)
            {
                OnConnectionLost(conn, new ExspeedConnectionException("connection closed"));
                return;
            }
            await RestoreAsync(conn).ConfigureAwait(false);
            if (ReferenceEquals(_conn, conn) && _state == State.Connected)
            {
                Raise(Reconnected, new ExspeedReconnectedEventArgs(conn.Info));
            }
            return;
        }
        List<Subscription> subs;
        List<CoreSubscription> coreSubs;
        lock (_stateGate)
        {
            if (_state != State.Reconnecting)
            {
                return;
            }
            _state = State.Closed;
            subs = _subs.ToList();
            coreSubs = _coreSubs.ToList();
        }
        var reason = new EndReason(ErrorCodes.Unavailable, $"connection lost: {lastErr.Message}");
        foreach (var s in subs)
        {
            s.End(reason, false);
        }
        foreach (var s in coreSubs)
        {
            s.End(reason, false);
        }
        Raise(Closed, new ExspeedErrorEventArgs(lastErr));
    }

    /// <summary>Re-create ephemeral consumers, then re-subscribe every live subscription (consumer and core).</summary>
    private async Task RestoreAsync(Connection conn)
    {
        List<ConsumerSpec> ephemeral;
        List<Subscription> subs;
        List<CoreSubscription> coreSubs;
        lock (_stateGate)
        {
            ephemeral = _ephemeral.Values.ToList();
            subs = _subs.ToList();
            coreSubs = _coreSubs.ToList();
        }
        foreach (var spec in ephemeral)
        {
            try
            {
                await conn.RequestAsync(new Request.CreateConsumer(spec.ToWireJson()), null, null, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (ExspeedConnectionException)
            {
                return; // lost again; the next loop retries
            }
            catch (ExspeedException)
            {
                // The re-subscribe below reports what went wrong.
            }
        }
        var tasks = new List<Task>();
        foreach (var sub in subs)
        {
            tasks.Add(ResubscribeAsync(conn, sub));
        }
        foreach (var sub in coreSubs)
        {
            tasks.Add(ResubscribeCoreAsync(conn, sub));
        }
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static async Task ResubscribeAsync(Connection conn, Subscription sub)
    {
        try
        {
            await conn.RequestAsync(new Request.Subscribe(sub.Consumer, sub.Window), null, sub, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (ExspeedConnectionException)
        {
            // Stays suspended for the next attempt.
        }
        catch (Exception e)
        {
            sub.End(new EndReason(e is ExspeedServerException se ? se.Code : ErrorCodes.Internal, e.Message), false);
        }
    }

    private static async Task ResubscribeCoreAsync(Connection conn, CoreSubscription sub)
    {
        try
        {
            await conn.RequestAsync(new Request.CoreSubscribe(sub.Subject, sub.Queue), null, sub, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (ExspeedConnectionException)
        {
            // Stays suspended for the next attempt.
        }
        catch (Exception e)
        {
            sub.End(new EndReason(e is ExspeedServerException se ? se.Code : ErrorCodes.Internal, e.Message), true);
        }
    }

    private static (string Host, int Port) ParseAddr(string addr, int fallbackPort)
    {
        int i = addr.LastIndexOf(':');
        if (i <= 0 || (addr.StartsWith('[') && addr.LastIndexOf(']') > i))
        {
            return (addr.Trim('[', ']'), fallbackPort);
        }
        string host = addr[..i].Trim('[', ']');
        return int.TryParse(addr[(i + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out int port)
            ? (host, port)
            : (host, fallbackPort);
    }

    /// <summary>
    /// Open a connection to the cluster leader. Without seed servers this is a plain connect to the configured
    /// host and port, except that a node naming another node as leader in its handshake is followed. With seeds,
    /// each candidate (the last known leader first) is asked whether it leads; leader hints are followed, and a
    /// follower is accepted only when no node claims to lead.
    /// </summary>
    private async Task<Connection> OpenLeaderAsync(string? hint, CancellationToken cancellationToken)
    {
        var queue = new LinkedList<string>();
        if (hint is not null)
        {
            queue.AddLast(hint);
        }
        if (_servers.Count > 0)
        {
            foreach (var s in _servers)
            {
                queue.AddLast(s);
            }
        }
        else
        {
            queue.AddLast($"{_connOpts.Host}:{_connOpts.Port}");
        }
        var tried = new HashSet<string>();
        Connection? fallback = null;
        Exception? lastErr = null;
        while (queue.First is { } first)
        {
            queue.RemoveFirst();
            string addr = first.Value;
            if (!tried.Add(addr))
            {
                continue;
            }
            var (host, port) = ParseAddr(addr, _connOpts.Port);
            Connection conn;
            try
            {
                conn = await Connection.OpenAsync(
                    _connOpts with { Host = host, Port = port },
                    OnConnectionLost,
                    OnAsyncError,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (ExspeedServerException e) when (e.Code is ErrorCodes.Unauthorized or ErrorCodes.Forbidden)
            {
                if (fallback is not null)
                {
                    _ = fallback.CloseAsync();
                }
                throw;
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                lastErr = e;
                continue;
            }
            bool isLeader = conn.Info.Leader is null;
            string? leader = conn.Info.Leader;
            if (_servers.Count > 0)
            {
                try
                {
                    var resp = await conn.RequestAsync(new Request.Metadata(), null, null, cancellationToken).ConfigureAwait(false);
                    if (resp is Response.Json j)
                    {
                        using var doc = JsonDocument.Parse(j.Body);
                        isLeader = WireJson.Bool(doc.RootElement, "is_leader");
                        leader = WireJson.Str(doc.RootElement, "leader");
                    }
                }
                catch (Exception e) when (e is ExspeedException || e is JsonException)
                {
                    // An old server without Metadata: trust the handshake.
                }
            }
            if (isLeader)
            {
                if (fallback is not null)
                {
                    _ = fallback.CloseAsync();
                }
                return conn;
            }
            if (leader is not null && !tried.Contains(leader))
            {
                queue.AddFirst(leader);
            }
            if (fallback is null)
            {
                fallback = conn;
            }
            else
            {
                _ = conn.CloseAsync();
            }
        }
        return fallback ?? throw (lastErr ?? new ExspeedConnectionException("no server reachable"));
    }

    // ---- request-reply inbox ---------------------------------------------------

    /// <summary>The connection's request-reply inbox: one core subscription to <c>&lt;prefix&gt;.*</c>.</summary>
    private sealed class Inbox : ISubscriptionSink
    {
        private readonly ExspeedClient _owner;

        public Inbox(ExspeedClient owner, string prefix)
        {
            _owner = owner;
            Prefix = prefix;
        }

        public string Prefix { get; }

        public Connection? Conn { get; private set; }

        public uint SubId { get; private set; }

        public Task Ready { get; set; } = Task.CompletedTask;

        public void OnSubscribed(Connection conn, uint subId)
        {
            bool current;
            lock (_owner._stateGate)
            {
                current = ReferenceEquals(_owner._inbox, this);
                if (current)
                {
                    Conn = conn;
                    SubId = subId;
                }
            }
            if (!current)
            {
                // Replaced (connection lost) while subscribing: release it.
                conn.RemoveSub(subId);
                conn.Send(new Request.Unsubscribe(subId));
            }
        }

        public void OnCoreMsg(Response.CoreMsg msg) => _owner.OnReply(msg);

        public void OnEnded(int code, string message)
        {
            bool current;
            lock (_owner._stateGate)
            {
                current = ReferenceEquals(_owner._inbox, this);
            }
            if (current)
            {
                _owner.DropInbox(new ExspeedServerException(code, message));
            }
        }
    }

    /// <summary>Subscribe this connection's inbox if needed; returns its subject prefix.</summary>
    private async Task<string> EnsureInboxAsync(CancellationToken cancellationToken)
    {
        Inbox inbox;
        lock (_stateGate)
        {
            if (_inbox is { } existing)
            {
                inbox = existing;
            }
            else
            {
                inbox = new Inbox(this, $"{InboxPrefix}.{Convert.ToHexString(RandomNumberGenerator.GetBytes(12)).ToLowerInvariant()}");
                _inbox = inbox;
                inbox.Ready = SubscribeInboxAsync(inbox);
            }
        }
        await inbox.Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
        return inbox.Prefix;
    }

    private async Task SubscribeInboxAsync(Inbox inbox)
    {
        try
        {
            await CallAsync<Response.SubscribeOk>(new Request.CoreSubscribe($"{inbox.Prefix}.*", null), null, inbox, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            lock (_stateGate)
            {
                if (ReferenceEquals(_inbox, inbox))
                {
                    _inbox = null; // the next request tries again
                }
            }
            throw;
        }
    }

    private void OnReply(Response.CoreMsg m)
    {
        string token = m.Subject[(m.Subject.LastIndexOf('.') + 1)..];
        TaskCompletionSource<CoreMessage>? tcs;
        lock (_replies)
        {
            if (!_replies.Remove(token, out tcs))
            {
                return; // late (timed out) or duplicate response
            }
        }
        tcs.TrySetResult(new CoreMessage(m, _hosts));
    }

    /// <summary>Forget the inbox (the next request subscribes a new one) and fail the requests waiting on it.</summary>
    private void DropInbox(Exception err)
    {
        Inbox? inbox;
        lock (_stateGate)
        {
            inbox = _inbox;
            _inbox = null;
        }
        if (inbox?.Conn is { IsClosed: false } c)
        {
            c.RemoveSub(inbox.SubId);
        }
        List<TaskCompletionSource<CoreMessage>> waiting;
        lock (_replies)
        {
            waiting = _replies.Values.ToList();
            _replies.Clear();
        }
        foreach (var w in waiting)
        {
            w.TrySetException(err);
        }
    }

    /// <summary>The internal interfaces the client offers to subscriptions, messages, buckets and publishers.</summary>
    private sealed class Hosts : ISubscriptionHost, ICoreHost, IKvHost, IPublisherTransport
    {
        private readonly ExspeedClient _c;

        public Hosts(ExspeedClient client)
        {
            _c = client;
        }

        public void Forget(Subscription sub)
        {
            lock (_c._stateGate)
            {
                _c._subs.Remove(sub);
            }
        }

        public void ForgetCore(CoreSubscription sub)
        {
            lock (_c._stateGate)
            {
                _c._coreSubs.Remove(sub);
            }
        }

        public void AckNoWait(string consumer, ulong offset) => _c.AckNoWait(consumer, offset);

        public Task NackAsync(string consumer, ulong offset, TimeSpan? delay, CancellationToken cancellationToken) =>
            _c.NackAsync(consumer, offset, delay, cancellationToken);

        public Task TermAsync(string consumer, ulong offset, string reason, CancellationToken cancellationToken) =>
            _c.TermAsync(consumer, offset, reason, cancellationToken);

        public Task InProgressAsync(string consumer, IReadOnlyList<ulong> offsets, CancellationToken cancellationToken) =>
            _c.InProgressAsync(consumer, offsets, cancellationToken);

        public Task PublishCoreAsync(string subject, ReadOnlyMemory<byte> value, CorePublishOptions? options, CancellationToken cancellationToken) =>
            _c.PublishCoreAsync(subject, value, options, cancellationToken);

        public Task<Response> RawRequestAsync(Request req, TimeSpan? timeout, CancellationToken cancellationToken) =>
            _c.RawRequestAsync(req, timeout, null, cancellationToken);

        public Task<Response.ReadResult> ReadRawAsync(string stream, ReadOptions options, CancellationToken cancellationToken) =>
            _c.ReadRawAsync(stream, options, cancellationToken);

        public Task<Response> SendAsync(Request req) => _c.RawRequestAsync(req, null, null, CancellationToken.None);
    }

}
