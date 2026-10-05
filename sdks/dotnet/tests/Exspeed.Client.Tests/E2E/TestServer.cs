using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Exspeed.Tests.E2E;

/// <summary>
/// Starts a real exspeed server for the end-to-end tests. The binary comes from <c>EXSPEED_BIN</c>, or else
/// <c>target/debug/exspeed</c> at the repository root (<c>cargo build -p exspeed --bin exspeed</c>). When neither
/// exists the e2e tests are skipped.
/// </summary>
internal sealed class TestServer : IAsyncDisposable
{
    private static readonly Lazy<string?> BinLazy = new(FindBinary);
    private readonly List<string> _log = new();
    private readonly TestServerOptions _opts;
    private Process _process;

    private TestServer(int port, int apiPort, string dataDir, TestServerOptions opts, Process process)
    {
        Port = port;
        ApiPort = apiPort;
        DataDir = dataDir;
        _opts = opts;
        _process = process;
        Attach(process);
    }

    /// <summary>The server binary, or <c>null</c> when there is none (the e2e tests are then skipped).</summary>
    public static string? Bin => BinLazy.Value;

    public static string DefaultBin => Path.Combine(RepoRoot() ?? "<repo>", "target", "debug", "exspeed");

    public static string SkipReason =>
        $"e2e tests skipped: set EXSPEED_BIN to an exspeed server binary, or build one with `cargo build -p exspeed --bin exspeed` (looked for {DefaultBin})";

    public int Port { get; }

    public int ApiPort { get; }

    public string DataDir { get; }

    private static string? RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Cargo.toml")) && Directory.Exists(Path.Combine(dir.FullName, "sdks")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return null;
    }

    private static string? FindBinary()
    {
        var env = Environment.GetEnvironmentVariable("EXSPEED_BIN");
        if (!string.IsNullOrEmpty(env))
        {
            var full = Path.GetFullPath(env);
            if (!File.Exists(full))
            {
                throw new InvalidOperationException($"EXSPEED_BIN={env} does not exist");
            }
            return full;
        }
        var bin = DefaultBin;
        return File.Exists(bin) ? bin : null;
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public static async Task<TestServer> StartAsync(TestServerOptions? opts = null)
    {
        opts ??= new TestServerOptions();
        int port = FreePort();
        int apiPort = FreePort();
        var dataDir = Directory.CreateTempSubdirectory("exspeed-dotnet-e2e-").FullName;
        var server = new TestServer(port, apiPort, dataDir, opts, await SpawnAsync(port, apiPort, dataDir, opts));
        await server.WaitReadyOrThrowAsync();
        return server;
    }

    private static async Task<Process> SpawnAsync(int port, int apiPort, string dataDir, TestServerOptions opts)
    {
        string bin = Bin ?? throw new InvalidOperationException(SkipReason);
        // The binary may be rebuilt underneath us; wait briefly for it to reappear.
        for (int i = 0; i < 300 && !File.Exists(bin); i++)
        {
            await Task.Delay(100);
        }
        var psi = new ProcessStartInfo(bin)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var a in new[] { "server", "--bind", $"127.0.0.1:{port}", "--api-bind", $"127.0.0.1:{apiPort}", "--data-dir", dataDir })
        {
            psi.ArgumentList.Add(a);
        }
        if (opts.AuthToken is not null)
        {
            psi.ArgumentList.Add("--auth-token");
            psi.ArgumentList.Add(opts.AuthToken);
        }
        if (opts.TlsCert is not null && opts.TlsKey is not null)
        {
            psi.ArgumentList.Add("--tls-cert");
            psi.ArgumentList.Add(opts.TlsCert);
            psi.ArgumentList.Add("--tls-key");
            psi.ArgumentList.Add(opts.TlsKey);
        }
        if (opts.TlsClientCa is not null)
        {
            psi.ArgumentList.Add("--tls-client-ca");
            psi.ArgumentList.Add(opts.TlsClientCa);
        }
        // Don't let the developer's environment change the server under test.
        foreach (var key in psi.Environment.Keys.Where(k => k.StartsWith("EXSPEED_", StringComparison.Ordinal)).ToList())
        {
            psi.Environment.Remove(key);
        }
        if (!psi.Environment.ContainsKey("RUST_LOG"))
        {
            psi.Environment["RUST_LOG"] = "warn";
        }
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return Process.Start(psi) ?? throw new InvalidOperationException("could not start the server");
            }
            catch (System.ComponentModel.Win32Exception) when (attempt < 50)
            {
                await Task.Delay(100); // "text file busy" while the binary is being replaced
            }
        }
    }

    private void Attach(Process p)
    {
        void Keep(object? _, DataReceivedEventArgs e)
        {
            if (e.Data is null)
            {
                return;
            }
            lock (_log)
            {
                _log.Add(e.Data);
                if (_log.Count > 200)
                {
                    _log.RemoveAt(0);
                }
            }
        }
        p.OutputDataReceived += Keep;
        p.ErrorDataReceived += Keep;
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
    }

    private async Task WaitReadyOrThrowAsync()
    {
        try
        {
            await WaitReadyAsync();
        }
        catch (Exception e)
        {
            await KillAsync();
            string log;
            lock (_log)
            {
                log = string.Join('\n', _log);
            }
            await StopAsync();
            throw new InvalidOperationException($"{e.Message}\n--- server log ---\n{log}");
        }
    }

    private async Task WaitReadyAsync()
    {
        bool tls = _opts.TlsCert is not null;
        using var handler = new HttpClientHandler
        {
            UseProxy = false,
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator,
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(1) };
        var url = $"{(tls ? "https" : "http")}://127.0.0.1:{ApiPort}/readyz";
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (_process.HasExited)
            {
                throw new InvalidOperationException($"server exited with code {_process.ExitCode}");
            }
            try
            {
                using var resp = await http.GetAsync(url);
                if (resp.StatusCode == HttpStatusCode.OK)
                {
                    return;
                }
            }
            catch (Exception)
            {
                // Not up yet.
            }
            await Task.Delay(50);
        }
        throw new InvalidOperationException("server did not become ready");
    }

    /// <summary>Connect a client to this server (reconnect off and no keepalive unless asked).</summary>
    public Task<ExspeedClient> ConnectAsync(ExspeedClientOptions? opts = null) =>
        ExspeedClient.ConnectAsync((opts ?? new ExspeedClientOptions { Reconnect = null }) with { Port = Port });

    /// <summary>Stop the process and start a new one on the same ports and data directory.</summary>
    public async Task RestartAsync()
    {
        await KillAsync();
        _process = await SpawnAsync(Port, ApiPort, DataDir, _opts);
        Attach(_process);
        await WaitReadyOrThrowAsync();
    }

    [DllImport("libc", SetLastError = true, EntryPoint = "kill")]
    private static extern int SysKill(int pid, int sig);

    private async Task KillAsync()
    {
        var p = _process;
        if (p.HasExited)
        {
            return;
        }
        bool signalled = false;
        if (!OperatingSystem.IsWindows())
        {
            try
            {
                signalled = SysKill(p.Id, 15) == 0; // SIGTERM: an orderly shutdown
            }
            catch (Exception)
            {
                signalled = false;
            }
        }
        if (!signalled)
        {
            p.Kill(entireProcessTree: true);
        }
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await p.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            p.Kill(entireProcessTree: true);
            await p.WaitForExitAsync();
        }
    }

    public async Task StopAsync()
    {
        await KillAsync();
        _process.Dispose();
        try
        {
            Directory.Delete(DataDir, recursive: true);
        }
        catch (Exception)
        {
            // Best effort.
        }
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    private static int _counter;

    /// <summary>A unique, valid stream/consumer name.</summary>
    public static string Uniq(string prefix) =>
        $"{prefix}-{Environment.ProcessId}-{DateTime.UtcNow.Ticks:x}-{Interlocked.Increment(ref _counter)}";

    /// <summary>Poll <paramref name="fn"/> until it returns a non-null (or true) value.</summary>
    public static async Task<T> Eventually<T>(Func<Task<T?>> fn, int timeoutMs = 10_000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var v = await fn();
                if (v is not null && !(v is bool b && !b))
                {
                    return v;
                }
            }
            catch (Exception e)
            {
                last = e;
            }
            await Task.Delay(50);
        }
        throw new TimeoutException($"eventually: timed out{(last is null ? "" : $" (last error: {last.Message})")}");
    }
}

internal sealed record TestServerOptions
{
    public string? AuthToken { get; init; }

    public string? TlsCert { get; init; }

    public string? TlsKey { get; init; }

    /// <summary>Require client certificates signed by this CA (mutual TLS).</summary>
    public string? TlsClientCa { get; init; }
}

/// <summary>A fact that is skipped (with the reason) when no server binary exists.</summary>
internal sealed class E2EFactAttribute : FactAttribute
{
    public E2EFactAttribute()
    {
        if (TestServer.Bin is null)
        {
            Skip = TestServer.SkipReason;
        }
    }
}

[CollectionDefinition("e2e", DisableParallelization = true)]
public sealed class E2ECollection
{
}

/// <summary>A shared server and client for a test class.</summary>
public sealed class ServerFixture : IAsyncLifetime
{
    internal TestServer Server { get; private set; } = null!;

    internal ExspeedClient Client { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (TestServer.Bin is null)
        {
            return;
        }
        Server = await TestServer.StartAsync();
        Client = await Server.ConnectAsync(new ExspeedClientOptions { ClientId = "e2e", Reconnect = null });
    }

    public async Task DisposeAsync()
    {
        if (Client is not null)
        {
            await Client.DisposeAsync();
        }
        if (Server is not null)
        {
            await Server.DisposeAsync();
        }
    }
}

/// <summary>Test certificates made with the BCL (no openssl needed).</summary>
internal sealed class TestCerts : IDisposable
{
    private TestCerts(string dir)
    {
        Dir = dir;
    }

    public string Dir { get; }

    public string CaPem => Path.Combine(Dir, "ca.pem");

    public string ServerCert => Path.Combine(Dir, "server.pem");

    public string ServerKey => Path.Combine(Dir, "server.key");

    public string ClientCert => Path.Combine(Dir, "client.pem");

    public string ClientKey => Path.Combine(Dir, "client.key");

    /// <summary>A CA, a server certificate for localhost/127.0.0.1 and a client certificate, all signed by the CA.</summary>
    public static TestCerts Create()
    {
        var dir = Directory.CreateTempSubdirectory("exspeed-dotnet-tls-").FullName;
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        var notAfter = DateTimeOffset.UtcNow.AddDays(1);

        using var caKey = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=exspeed-test-ca", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        caReq.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caReq.PublicKey, false));
        using var ca = caReq.CreateSelfSigned(notBefore, notAfter);
        File.WriteAllText(Path.Combine(dir, "ca.pem"), ca.ExportCertificatePem());

        void Leaf(string name, string cn, string eku, Action<SubjectAlternativeNameBuilder> san)
        {
            using var key = RSA.Create(2048);
            var req = new CertificateRequest($"CN={cn}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            req.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
            req.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
            req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new Oid(eku) }, false));
            req.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(req.PublicKey, false));
            req.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(ca, true, false));
            var b = new SubjectAlternativeNameBuilder();
            san(b);
            req.CertificateExtensions.Add(b.Build());
            var serial = RandomNumberGenerator.GetBytes(16);
            serial[0] &= 0x7f;
            using var cert = req.Create(ca, notBefore, notAfter, serial);
            File.WriteAllText(Path.Combine(dir, $"{name}.pem"), cert.ExportCertificatePem());
            File.WriteAllText(Path.Combine(dir, $"{name}.key"), key.ExportPkcs8PrivateKeyPem());
        }

        Leaf("server", "localhost", "1.3.6.1.5.5.7.3.1", b =>
        {
            b.AddDnsName("localhost");
            b.AddIpAddress(IPAddress.Loopback);
        });
        Leaf("client", "orders.internal", "1.3.6.1.5.5.7.3.2", b => b.AddDnsName("orders.internal"));
        return new TestCerts(dir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Dir, recursive: true);
        }
        catch (Exception)
        {
            // Best effort.
        }
    }
}
