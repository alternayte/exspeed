using System.Net;
using System.Net.Sockets;
using Exspeed.Protocol;

namespace Exspeed.Tests;

internal sealed record Received(uint Corr, Request Req);

/// <summary>One accepted client connection on the fake server.</summary>
internal sealed class FakeConn
{
    private readonly FakeServer _server;
    private readonly object _writeGate = new();
    private readonly List<Received> _received = new();

    public FakeConn(TcpClient client, FakeServer server)
    {
        Client = client;
        _server = server;
        Stream = client.GetStream();
    }

    public TcpClient Client { get; }

    public NetworkStream Stream { get; }

    /// <summary>Every request received so far, in order.</summary>
    public IReadOnlyList<Received> Received
    {
        get
        {
            lock (_received)
            {
                return _received.ToList();
            }
        }
    }

    /// <summary>The requests of one type.</summary>
    public List<(uint Corr, T Req)> Of<T>()
        where T : Request =>
        Received.Where(r => r.Req is T).Select(r => (r.Corr, (T)r.Req)).ToList();

    public void Reply(uint corr, Response resp) => Write(resp.ToFrame(corr));

    /// <summary>Write several frames in a single chunk.</summary>
    public void ReplyMany(params (uint Corr, Response Resp)[] frames) =>
        Write(frames.SelectMany(f => f.Resp.ToFrame(f.Corr)).ToArray());

    private void Write(byte[] bytes)
    {
        lock (_writeGate)
        {
            try
            {
                Stream.Write(bytes);
            }
            catch (Exception)
            {
                // The client went away.
            }
        }
    }

    /// <summary>Drop the connection abruptly.</summary>
    public void Destroy()
    {
        try
        {
            Client.Client.LingerState = new LingerOption(true, 0);
            Client.Close();
        }
        catch (Exception)
        {
            // Already closed.
        }
    }

    public async Task RunAsync()
    {
        var parser = new FrameParser();
        var frames = new List<Frame>();
        var buf = new byte[64 * 1024];
        try
        {
            while (true)
            {
                int n = await Stream.ReadAsync(buf);
                if (n == 0)
                {
                    return;
                }
                frames.Clear();
                frames.AddRange(parser.Push(buf.AsSpan(0, n)));
                foreach (var f in frames)
                {
                    Request req;
                    try
                    {
                        req = Request.Decode(f.Opcode, f.Payload);
                    }
                    catch (Exception e)
                    {
                        Reply(f.CorrelationId, new Response.Error(400, e.Message, null));
                        continue;
                    }
                    lock (_received)
                    {
                        _received.Add(new Received(f.CorrelationId, req));
                    }
                    _server.Handle(this, f.CorrelationId, req);
                }
            }
        }
        catch (Exception)
        {
            // Connection closed.
        }
    }
}

/// <summary>Handles a request; return true to suppress the default answers (Connect, Ping).</summary>
internal delegate bool FakeHandler(FakeConn conn, uint corr, Request req);

/// <summary>
/// A scriptable protocol-v2 server for unit tests. Connect and Ping are answered automatically unless the
/// handler returns true for them.
/// </summary>
internal sealed class FakeServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly List<FakeConn> _conns = new();
    private readonly CancellationTokenSource _cts = new();

    private FakeServer(TcpListener listener)
    {
        _listener = listener;
    }

    public FakeHandler Handler { get; set; } = (_, _, _) => false;

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public IReadOnlyList<FakeConn> Conns
    {
        get
        {
            lock (_conns)
            {
                return _conns.ToList();
            }
        }
    }

    public FakeConn Last => Conns[^1];

    public static FakeServer Start(FakeHandler? handler = null)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var fake = new FakeServer(listener);
        if (handler is not null)
        {
            fake.Handler = handler;
        }
        _ = fake.AcceptLoopAsync();
        return fake;
    }

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_cts.Token);
                client.NoDelay = true;
                var conn = new FakeConn(client, this);
                lock (_conns)
                {
                    _conns.Add(conn);
                }
                _ = Task.Run(conn.RunAsync);
            }
        }
        catch (Exception)
        {
            // Stopped.
        }
    }

    public void Handle(FakeConn conn, uint corr, Request req)
    {
        if (Handler(conn, corr, req))
        {
            return;
        }
        if (req is Request.Connect)
        {
            conn.Reply(corr, new Response.ConnectOk("test", "n1", null));
        }
        else if (req is Request.Ping)
        {
            conn.Reply(corr, new Response.Pong());
        }
    }

    /// <summary>Wait until <paramref name="cond"/> holds.</summary>
    public static async Task Until(Func<bool> cond, int timeoutMs = 3000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!cond())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("FakeServer.Until: timed out");
            }
            await Task.Delay(10);
        }
    }

    public ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        foreach (var c in Conns)
        {
            c.Destroy();
        }
        return ValueTask.CompletedTask;
    }
}
