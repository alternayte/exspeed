using static Exspeed.Tests.E2E.TestServer;

namespace Exspeed.Tests.E2E;

/// <summary>Token auth, TLS, mutual TLS and reconnection against a real server.</summary>
[Collection("e2e")]
public sealed class ConnectionE2ETests
{
    [E2EFact]
    public async Task RejectsAWrongOrMissingTokenAndAcceptsTheRightOne()
    {
        await using var server = await TestServer.StartAsync(new TestServerOptions { AuthToken = "s3cret-token" });
        foreach (var token in new[] { "wrong", null })
        {
            var err = await Assert.ThrowsAsync<ExspeedServerException>(() => server.ConnectAsync(new ExspeedClientOptions { Token = token, Reconnect = null }));
            Assert.Equal(401, err.Code);
        }
        await using var c = await server.ConnectAsync(new ExspeedClientOptions { Token = "s3cret-token", Reconnect = null });
        var s = Uniq("authed");
        await c.CreateStreamAsync(s);
        Assert.Equal(0UL, (await c.PublishAsync(s, "auth.ok", "yes")).Offset);
        Assert.Equal(1, Assert.Single(Assert.Single((await c.QueryAsync($"SELECT COUNT(*) AS n FROM \"{s}\"")).Rows)).GetInt32());
    }

    [E2EFact]
    public async Task ResubscribesAfterTheServerRestartsAndRedeliversUnackedRecords()
    {
        await using var server = await TestServer.StartAsync();
        await using var client = await server.ConnectAsync(new ExspeedClientOptions
        {
            Reconnect = new ReconnectOptions { InitialDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromMilliseconds(200) },
        });
        var s = Uniq("durable");
        var c = Uniq("durable-c");
        await client.CreateStreamAsync(s);
        await client.CreateConsumerAsync(new ConsumerSpec(c, s));
        await client.PublishAsync(s, "work.item", "before");
        var sub = await client.SubscribeAsync(c, new SubscribeOptions { Window = 10 });
        var m1 = (await sub.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal("before", m1.Text()); // not acked

        var events = new List<string>();
        var reconnected = new TaskCompletionSource();
        client.Disconnected += (_, _) =>
        {
            lock (events)
            {
                events.Add("disconnect");
            }
        };
        client.Reconnected += (_, _) => reconnected.TrySetResult();
        await server.RestartAsync();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(new[] { "disconnect" }, events);
        Assert.True(client.IsConnected);

        var again = (await sub.NextAsync(TimeSpan.FromSeconds(10)))!;
        Assert.Equal("before", again.Text());
        // (The delivery count may restart at 1: the server persists consumer state in periodic snapshots.)
        again.Ack();
        await client.PublishAsync(s, "work.item", "after");
        var m2 = (await sub.NextAsync(TimeSpan.FromSeconds(5)))!;
        Assert.Equal("after", m2.Text());
        m2.Ack();
        Assert.False(sub.IsClosed);
    }

    [E2EFact]
    public async Task FailsRequestsWithConnectionExceptionOnceDisconnectedWithReconnectOff()
    {
        await using var server = await TestServer.StartAsync();
        await using var client = await server.ConnectAsync();
        var closed = new TaskCompletionSource();
        client.Closed += (_, _) => closed.TrySetResult();
        await server.RestartAsync();
        await closed.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => client.PingAsync());
    }

    [E2EFact]
    public async Task ConnectsOverTlsWithACustomCaAndRefusesUntrustedCertificatesAndPlainTcp()
    {
        using var certs = TestCerts.Create();
        await using var server = await TestServer.StartAsync(new TestServerOptions { TlsCert = certs.ServerCert, TlsKey = certs.ServerKey });

        await using (var c = await server.ConnectAsync(new ExspeedClientOptions { Tls = ExspeedTlsOptions.FromPem(certs.CaPem), Reconnect = null }))
        {
            var s = Uniq("tls");
            await c.CreateStreamAsync(s);
            Assert.Equal(0UL, (await c.PublishAsync(s, "tls.ok", "secure")).Offset);
        }
        // By host name too (the certificate names localhost).
        await using (var c = await ExspeedClient.ConnectAsync(new ExspeedClientOptions
        {
            Host = "localhost",
            Port = server.Port,
            Tls = ExspeedTlsOptions.FromPem(certs.CaPem),
            Reconnect = null,
        }))
        {
            await c.PingAsync();
        }

        var quick = TimeSpan.FromSeconds(3);
        // The system CAs don't trust the test CA.
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => server.ConnectAsync(new ExspeedClientOptions { Tls = new ExspeedTlsOptions(), RequestTimeout = quick, Reconnect = null }));
        // A certificate for another name.
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => server.ConnectAsync(new ExspeedClientOptions
        {
            Tls = ExspeedTlsOptions.FromPem(certs.CaPem) with { ServerName = "elsewhere.example" },
            RequestTimeout = quick,
            Reconnect = null,
        }));
        await Assert.ThrowsAsync<ExspeedConnectionException>(() => server.ConnectAsync(new ExspeedClientOptions { RequestTimeout = quick, Reconnect = null }));
    }

    [E2EFact]
    public async Task MutualTlsConnectsWithAClientCertificateAndIsRefusedWithoutOne()
    {
        using var certs = TestCerts.Create();
        await using var server = await TestServer.StartAsync(new TestServerOptions
        {
            TlsCert = certs.ServerCert,
            TlsKey = certs.ServerKey,
            TlsClientCa = certs.CaPem,
        });
        await using (var c = await server.ConnectAsync(new ExspeedClientOptions
        {
            Tls = ExspeedTlsOptions.FromPem(certs.CaPem, certs.ClientCert, certs.ClientKey),
            Reconnect = null,
        }))
        {
            var s = Uniq("mtls");
            await c.CreateStreamAsync(s);
            Assert.Equal(0UL, (await c.PublishAsync(s, "mtls.ok", "mutual")).Offset);
        }
        await Assert.ThrowsAsync<ExspeedConnectionException>(async () =>
        {
            // TLS 1.3 reports a missing client certificate after the handshake, so the refusal may only show
            // when the first request (the Connect handshake) is answered by a closed connection.
            await using var c = await server.ConnectAsync(new ExspeedClientOptions
            {
                Tls = ExspeedTlsOptions.FromPem(certs.CaPem),
                RequestTimeout = TimeSpan.FromSeconds(3),
                Reconnect = null,
            });
        });
    }
}
