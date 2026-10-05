using System.Collections.Concurrent;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>Receives a subscription's pushes (<c>Deliver</c> for consumers, <c>CoreMsg</c> for core subscriptions).</summary>
internal interface ISubscriptionSink
{
    /// <summary>Called on the reader when <c>SubscribeOk</c> arrives, before any push for it is routed.</summary>
    void OnSubscribed(Connection conn, uint subId);

    void OnDeliver(IReadOnlyList<WireRecord> records)
    {
    }

    void OnCoreMsg(Response.CoreMsg msg)
    {
    }

    void OnEnded(int code, string message);
}

internal sealed record ConnectionOptions(
    string Host,
    int Port,
    ExspeedTlsOptions? Tls,
    string ClientId,
    string? Token,
    TimeSpan RequestTimeout,
    TimeSpan Keepalive);

/// <summary>
/// One TCP (or TLS) connection: handshake, frame parsing, correlation-id multiplexing, push routing and
/// keepalive. Reconnection lives one level up, in <see cref="ExspeedClient"/>.
/// </summary>
internal sealed class Connection
{
    private sealed class Pending
    {
        public readonly TaskCompletionSource<Response> Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ISubscriptionSink? Sink;
    }

    private readonly ConnectionOptions _opts;
    private readonly Socket _socket;
    private readonly Stream _stream;
    private readonly Action<Connection, Exception> _onClose;
    private readonly Action<Exception> _onAsyncError;
    private readonly ConcurrentDictionary<uint, Pending> _pending = new();
    private readonly ConcurrentDictionary<uint, ISubscriptionSink> _subs = new();
    private readonly Channel<byte[]> _out = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true });
    private readonly object _gate = new();
    private readonly TaskCompletionSource _closedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _nextCorr;
    private bool _closed;
    private bool _closedByUser;
    private Timer? _keepalive;
    private Task _writer = Task.CompletedTask;
    private ServerInfo? _info;

    private Connection(ConnectionOptions opts, Socket socket, Stream stream, Action<Connection, Exception> onClose, Action<Exception> onAsyncError)
    {
        _opts = opts;
        _socket = socket;
        _stream = stream;
        _onClose = onClose;
        _onAsyncError = onAsyncError;
    }

    public ServerInfo Info => _info ?? throw new InvalidOperationException("not connected");

    public bool IsClosed
    {
        get
        {
            lock (_gate)
            {
                return _closed;
            }
        }
    }

    /// <summary>Completes when the connection is torn down.</summary>
    public Task Closed => _closedTcs.Task;

    /// <summary>Open a socket and run the <c>Connect</c> handshake.</summary>
    public static async Task<Connection> OpenAsync(
        ConnectionOptions opts,
        Action<Connection, Exception> onClose,
        Action<Exception> onAsyncError,
        CancellationToken cancellationToken)
    {
        var (socket, stream) = await OpenStreamAsync(opts, cancellationToken).ConfigureAwait(false);
        var conn = new Connection(opts, socket, stream, onClose, onAsyncError);
        conn.Start();
        try
        {
            var resp = await conn.RequestAsync(new Request.Connect(opts.ClientId, opts.Token), null, null, cancellationToken)
                .ConfigureAwait(false);
            if (resp is not Response.ConnectOk ok)
            {
                throw new ExspeedProtocolException($"unexpected handshake reply {resp.Name}");
            }
            conn._info = new ServerInfo(ok.ServerVersion, ok.NodeId, ok.Leader);
        }
        catch (Exception err)
        {
            lock (conn._gate)
            {
                conn._closedByUser = true;
            }
            conn.Teardown(err);
            throw;
        }
        conn.StartKeepalive();
        return conn;
    }

    private static async Task<(Socket, Stream)> OpenStreamAsync(ConnectionOptions opts, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (opts.RequestTimeout > TimeSpan.Zero)
        {
            cts.CancelAfter(opts.RequestTimeout);
        }
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        Stream? stream = null;
        string target = $"{opts.Host}:{opts.Port}";
        try
        {
            await socket.ConnectAsync(opts.Host, opts.Port, cts.Token).ConfigureAwait(false);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            stream = new NetworkStream(socket, ownsSocket: true);
            if (opts.Tls is { } tls)
            {
                var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                stream = ssl;
                await ssl.AuthenticateAsClientAsync(TlsOptions(opts.Host, tls), cts.Token).ConfigureAwait(false);
            }
            return (socket, stream);
        }
        catch (Exception e)
        {
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }
            socket.Dispose();
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            if (e is OperationCanceledException)
            {
                throw new ExspeedConnectionException($"connect to {target} timed out");
            }
            throw new ExspeedConnectionException($"connect to {target} failed: {e.Message}", e);
        }
    }

    private static SslClientAuthenticationOptions TlsOptions(string host, ExspeedTlsOptions tls)
    {
        var o = new SslClientAuthenticationOptions
        {
            TargetHost = tls.ServerName ?? host,
            EnabledSslProtocols = tls.Protocols,
            RemoteCertificateValidationCallback = tls.RemoteCertificateValidation,
        };
        if (tls.CaCertificates is { Count: > 0 } ca)
        {
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck,
            };
            policy.CustomTrustStore.AddRange(ca);
            o.CertificateChainPolicy = policy;
        }
        if (tls.ClientCertificate is { } cert)
        {
            o.ClientCertificates = new X509CertificateCollection { cert };
            o.LocalCertificateSelectionCallback = (_, _, _, _, _) => cert;
        }
        return o;
    }

    private void Start()
    {
        _writer = Task.Run(WriteLoopAsync);
        _ = Task.Run(ReadLoopAsync);
    }

    /// <summary>
    /// Send a request and wait for its response. The frame is queued before this method returns, so wire order
    /// matches call order. Error responses fail the task with <see cref="ExspeedServerException"/>.
    /// </summary>
    public Task<Response> RequestAsync(Request req, TimeSpan? timeout, ISubscriptionSink? sink, CancellationToken cancellationToken)
    {
        uint corr = AllocCorr();
        byte[] frame;
        try
        {
            frame = req.ToFrame(corr);
        }
        catch (Exception e)
        {
            return Task.FromException<Response>(e);
        }
        var p = new Pending { Sink = sink };
        lock (_gate)
        {
            if (_closed)
            {
                return Task.FromException<Response>(new ExspeedConnectionException("connection closed"));
            }
            _pending[corr] = p;
            _out.Writer.TryWrite(frame);
        }
        return AwaitAsync(p, corr, req, timeout ?? _opts.RequestTimeout, cancellationToken);
    }

    private async Task<Response> AwaitAsync(Pending p, uint corr, Request req, TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            if (timeout > TimeSpan.Zero)
            {
                var max = TimeSpan.FromMilliseconds(int.MaxValue - 1);
                return await p.Tcs.Task.WaitAsync(timeout < max ? timeout : max, cancellationToken).ConfigureAwait(false);
            }
            return await p.Tcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _pending.TryRemove(corr, out _);
            throw new ExspeedTimeoutException($"{req.Name} timed out after {(long)timeout.TotalMilliseconds} ms");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _pending.TryRemove(corr, out _);
            throw;
        }
    }

    /// <summary>
    /// Send with correlation id 0 (fire-and-forget): no reply on success; a failure arrives as an async error.
    /// Returns false when not sent.
    /// </summary>
    public bool Send(Request req)
    {
        var frame = req.ToFrame(0);
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }
            return _out.Writer.TryWrite(frame);
        }
    }

    /// <summary>Stop routing pushes for a subscription.</summary>
    public void RemoveSub(uint subId) => _subs.TryRemove(subId, out _);

    /// <summary>Close gracefully: flush what was queued (acks), then drop the socket.</summary>
    public async Task CloseAsync()
    {
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }
            _closedByUser = true;
            _out.Writer.TryComplete();
        }
        try
        {
            await _writer.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Flushing failed or took too long; drop the socket anyway.
        }
        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (Exception)
        {
            // Already gone.
        }
        Teardown(new ExspeedConnectionException("connection closed"));
    }

    /// <summary>Drop the connection now (e.g. a keepalive ping timed out).</summary>
    public void Abort(Exception err) => Teardown(err);

    private uint AllocCorr()
    {
        while (true)
        {
            uint c = (uint)Interlocked.Increment(ref _nextCorr);
            if (c != 0)
            {
                return c;
            }
        }
    }

    private async Task WriteLoopAsync()
    {
        var batch = new MemoryStream(64 * 1024);
        try
        {
            var reader = _out.Reader;
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                batch.SetLength(0);
                while (batch.Length < 256 * 1024 && reader.TryRead(out var frame))
                {
                    batch.Write(frame, 0, frame.Length);
                }
                await _stream.WriteAsync(batch.GetBuffer().AsMemory(0, (int)batch.Length)).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            Teardown(new ExspeedConnectionException($"connection error: {e.Message}", e));
        }
    }

    private async Task ReadLoopAsync()
    {
        var parser = new FrameParser();
        var frames = new List<Frame>();
        try
        {
            while (true)
            {
                var buf = parser.GetWriteBuffer(16 * 1024);
                int n = await _stream.ReadAsync(buf).ConfigureAwait(false);
                if (n == 0)
                {
                    throw new ExspeedConnectionException("connection closed");
                }
                parser.Advance(n);
                frames.Clear();
                parser.Drain(frames);
                foreach (var f in frames)
                {
                    lock (_gate)
                    {
                        if (_closed)
                        {
                            return;
                        }
                    }
                    Dispatch(f);
                }
            }
        }
        catch (Exception e)
        {
            Teardown(e is ExspeedException ? e : new ExspeedConnectionException($"connection error: {e.Message}", e));
        }
    }

    private void Dispatch(Frame f)
    {
        Response resp;
        try
        {
            resp = Response.Decode(f.Opcode, f.Payload);
        }
        catch (ExspeedProtocolException err)
        {
            if (f.CorrelationId != 0 && _pending.TryRemove(f.CorrelationId, out var p))
            {
                p.Tcs.TrySetException(err);
            }
            else
            {
                SafeAsyncError(err);
            }
            return;
        }
        Route(f.CorrelationId, resp);
    }

    private void Route(uint corr, Response resp)
    {
        if (corr == 0)
        {
            switch (resp)
            {
                case Response.Deliver d:
                    if (_subs.TryGetValue(d.SubId, out var ds))
                    {
                        ds.OnDeliver(d.Records);
                    }
                    return;
                case Response.CoreMsg m:
                    if (_subs.TryGetValue(m.SubId, out var ms))
                    {
                        ms.OnCoreMsg(m);
                    }
                    return;
                case Response.SubscriptionEnded e:
                    if (_subs.TryRemove(e.SubId, out var es))
                    {
                        es.OnEnded(e.Code, e.Message);
                    }
                    return;
                case Response.Error err:
                    SafeAsyncError(err.ToException());
                    return;
                default:
                    return;
            }
        }
        _pending.TryRemove(corr, out var p);
        if (resp is Response.SubscribeOk ok)
        {
            if (p?.Sink is { } sink)
            {
                // Register before completing so a Deliver behind it in the same chunk is not lost.
                _subs[ok.SubId] = sink;
                sink.OnSubscribed(this, ok.SubId);
            }
            else
            {
                // The subscribe call timed out or was abandoned; release the server side.
                Send(new Request.Unsubscribe(ok.SubId));
            }
        }
        if (p is null)
        {
            return;
        }
        if (resp is Response.Error error)
        {
            p.Tcs.TrySetException(error.ToException());
        }
        else
        {
            p.Tcs.TrySetResult(resp);
        }
    }

    private void SafeAsyncError(Exception err)
    {
        try
        {
            _onAsyncError(err);
        }
        catch (Exception)
        {
            // A handler's failure must not break the reader.
        }
    }

    private void StartKeepalive()
    {
        if (_opts.Keepalive <= TimeSpan.Zero)
        {
            return;
        }
        _keepalive = new Timer(_ => _ = PingAsync(), null, _opts.Keepalive, _opts.Keepalive);
    }

    private async Task PingAsync()
    {
        try
        {
            await RequestAsync(new Request.Ping(), null, null, CancellationToken.None).ConfigureAwait(false);
        }
        catch (ExspeedTimeoutException)
        {
            // A ping that times out means the peer is gone (half-open socket).
            Abort(new ExspeedConnectionException("keepalive ping timed out"));
        }
        catch (Exception)
        {
            // The connection is closing; teardown reports it.
        }
    }

    private void Teardown(Exception err)
    {
        Pending[] pending;
        bool notify;
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }
            _closed = true;
            notify = !_closedByUser;
            pending = _pending.Values.ToArray();
            _pending.Clear();
            _subs.Clear();
            _out.Writer.TryComplete();
        }
        _keepalive?.Dispose();
        _keepalive = null;
        try
        {
            _stream.Dispose();
        }
        catch (Exception)
        {
            // Ignore errors while disposing a broken stream.
        }
        try
        {
            _socket.Dispose();
        }
        catch (Exception)
        {
            // Ignore.
        }
        var connErr = err as ExspeedConnectionException ?? new ExspeedConnectionException(err.Message, err);
        foreach (var p in pending)
        {
            p.Tcs.TrySetException(err is ExspeedServerException ? err : connErr);
        }
        _closedTcs.TrySetResult();
        if (notify)
        {
            try
            {
                _onClose(this, err);
            }
            catch (Exception)
            {
                // Never let a handler break teardown.
            }
        }
    }
}
