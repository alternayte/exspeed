using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Exspeed;

/// <summary>Options for <see cref="ExspeedClient.ConnectAsync"/>.</summary>
public sealed record ExspeedClientOptions
{
    /// <summary>Server host. Default <c>127.0.0.1</c>.</summary>
    public string Host { get; init; } = "127.0.0.1";

    /// <summary>Server port. Default 5933.</summary>
    public int Port { get; init; } = ProtocolConstants.DefaultPort;

    /// <summary>
    /// Cluster seed addresses (<c>host:port</c>). When set, the client connects to whichever node is the leader,
    /// following the leader hints followers return, and finds the new leader again after a failover. Overrides
    /// <see cref="Host"/> and <see cref="Port"/>.
    /// </summary>
    public IReadOnlyList<string>? Servers { get; init; }

    /// <summary>Bearer token, when the server runs with auth.</summary>
    public string? Token { get; init; }

    /// <summary>TLS settings; <c>null</c> (the default) connects over plain TCP.</summary>
    public ExspeedTlsOptions? Tls { get; init; }

    /// <summary>Sent in the handshake and shown in server logs. Default <c>exspeed-dotnet</c>.</summary>
    public string ClientId { get; init; } = "exspeed-dotnet";

    /// <summary>
    /// How long to wait for a response (on top of a pull's or read's own wait). Also bounds connecting.
    /// Default 30 s.
    /// </summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Ping interval; the server drops connections idle for 120 s. <see cref="TimeSpan.Zero"/> disables pings.
    /// Default 20 s.
    /// </summary>
    public TimeSpan Keepalive { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Reconnect automatically when the connection drops (on by default). Pending requests fail with
    /// <see cref="ExspeedConnectionException"/>; subscriptions are re-established. <c>null</c> disables
    /// reconnection: the client closes when the connection drops.
    /// </summary>
    public ReconnectOptions? Reconnect { get; init; } = new();
}

/// <summary>How the client reconnects after the connection drops.</summary>
public sealed record ReconnectOptions
{
    /// <summary>Give up after this many failed attempts in a row. Default: unlimited.</summary>
    public int MaxAttempts { get; init; } = int.MaxValue;

    /// <summary>Delay before the first attempt; it doubles each attempt (with jitter). Default 100 ms.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Upper bound for the delay between attempts. Default 5 s.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>TLS settings. Certificates are verified by default.</summary>
public sealed record ExspeedTlsOptions
{
    /// <summary>
    /// Trust only these CA certificates (a private CA) instead of the system store. <c>null</c> = system CAs.
    /// </summary>
    public X509Certificate2Collection? CaCertificates { get; init; }

    /// <summary>
    /// A client certificate with its private key, for servers that require mutual TLS.
    /// See <see cref="FromPem(string?, string?, string?)"/>.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; init; }

    /// <summary>The name to verify the server's certificate against (and to send as SNI). Default: the host.</summary>
    public string? ServerName { get; init; }

    /// <summary>
    /// Replaces the default certificate check entirely. When set, <see cref="CaCertificates"/> is still used to
    /// build the chain, and this callback decides.
    /// </summary>
    public RemoteCertificateValidationCallback? RemoteCertificateValidation { get; init; }

    /// <summary>Allowed TLS versions. Default: the operating system's choice.</summary>
    public System.Security.Authentication.SslProtocols Protocols { get; init; } =
        System.Security.Authentication.SslProtocols.None;

    /// <summary>
    /// Build TLS options from PEM files: a CA bundle to trust, and optionally a client certificate and its key
    /// for mutual TLS.
    /// </summary>
    /// <param name="caFile">PEM file of the CA certificate(s) to trust, or <c>null</c> for the system CAs.</param>
    /// <param name="certFile">PEM file of the client certificate, or <c>null</c>.</param>
    /// <param name="keyFile">PEM file of the client certificate's private key (needed with <paramref name="certFile"/>).</param>
    /// <returns>The options.</returns>
    public static ExspeedTlsOptions FromPem(string? caFile, string? certFile = null, string? keyFile = null)
    {
        X509Certificate2Collection? ca = null;
        if (caFile is not null)
        {
            ca = new X509Certificate2Collection();
            ca.ImportFromPemFile(caFile);
        }
        X509Certificate2? client = null;
        if (certFile is not null)
        {
            client = LoadClientCertificate(X509Certificate2.CreateFromPemFile(certFile, keyFile));
        }
        return new ExspeedTlsOptions { CaCertificates = ca, ClientCertificate = client };
    }

    /// <summary>Build TLS options from PEM text (see <see cref="FromPem(string?, string?, string?)"/>).</summary>
    /// <param name="caPem">PEM text of the CA certificate(s) to trust, or <c>null</c> for the system CAs.</param>
    /// <param name="certPem">PEM text of the client certificate, or <c>null</c>.</param>
    /// <param name="keyPem">PEM text of the client certificate's private key.</param>
    /// <returns>The options.</returns>
    public static ExspeedTlsOptions FromPemText(string? caPem, string? certPem = null, string? keyPem = null)
    {
        X509Certificate2Collection? ca = null;
        if (caPem is not null)
        {
            ca = new X509Certificate2Collection();
            ca.ImportFromPem(caPem);
        }
        X509Certificate2? client = null;
        if (certPem is not null)
        {
            client = LoadClientCertificate(X509Certificate2.CreateFromPem(certPem, keyPem ?? certPem));
        }
        return new ExspeedTlsOptions { CaCertificates = ca, ClientCertificate = client };
    }

    /// <summary>
    /// A certificate whose key SslStream can use on every platform (an ephemeral PEM key is not usable on
    /// Windows, so round-trip it through PKCS#12).
    /// </summary>
    private static X509Certificate2 LoadClientCertificate(X509Certificate2 pem)
    {
        using (pem)
        {
            return new X509Certificate2(pem.Export(X509ContentType.Pkcs12));
        }
    }
}
