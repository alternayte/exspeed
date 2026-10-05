package io.exspeed.client.e2e;

import static io.exspeed.client.e2e.TestServer.openssl;
import static io.exspeed.client.e2e.TestServer.uniq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.exspeed.client.ConnectionException;
import io.exspeed.client.ExspeedClient;
import io.exspeed.client.TlsOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** TLS and mutual TLS against a real server, with certificates made by openssl. */
class E2eTlsTest {
  static void assumeTools() {
    TestServer.assumeAvailable();
    Assumptions.assumeTrue(TestServer.hasOpenssl(), "TLS e2e tests skipped: openssl not found");
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class ServerTls {
    TestServer server;
    Path dir;

    @BeforeAll
    void start() throws Exception {
      assumeTools();
      dir = Files.createTempDirectory("exspeed-java-tls-");
      openssl(dir, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", "key.pem", "-out",
          "cert.pem", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1");
      server = TestServer.start(new TestServer.Options(null, dir.resolve("cert.pem"), dir.resolve("key.pem"), null));
    }

    @AfterAll
    void stop() {
      if (server != null) {
        server.close();
      }
      TestServer.deleteTree(dir);
    }

    @Test
    void connectsOverTlsWithACustomCa() {
      TlsOptions tls = TlsOptions.builder().caPem(dir.resolve("cert.pem")).build();
      try (ExspeedClient c = server.connect(b -> b.tls(tls))) {
        String s = uniq("tls");
        c.createStream(s);
        assertEquals(0, c.publish(s, "tls.ok", "secure").offset());
      }
      // By host name too (the certificate names localhost).
      try (ExspeedClient c = server.connect(b -> b.host("localhost").tls(tls))) {
        c.ping();
      }
    }

    @Test
    void refusesAnUntrustedCertificateAndPlainTcp() {
      assertThrows(ConnectionException.class, () -> server.connect(b -> b.tls(TlsOptions.systemDefault())
          .requestTimeout(Duration.ofSeconds(3))));
      assertThrows(ConnectionException.class, () -> server.connect(b -> b.requestTimeout(Duration.ofSeconds(3))));
    }

    @Test
    void refusesACertificateForAnotherName() {
      TlsOptions wrongName = TlsOptions.builder().caPem(dir.resolve("cert.pem")).serverName("example.com").build();
      assertThrows(ConnectionException.class, () -> server.connect(b -> b.tls(wrongName)
          .requestTimeout(Duration.ofSeconds(3))));
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class MutualTls {
    TestServer server;
    Path dir;

    @BeforeAll
    void start() throws Exception {
      assumeTools();
      dir = Files.createTempDirectory("exspeed-java-mtls-");
      // One CA signs both the server's and the client's certificate.
      openssl(dir, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", "ca.key", "-out",
          "ca.pem", "-subj", "/CN=exspeed-test-ca");
      String[][] certs = {
        {"server", "localhost", "subjectAltName=DNS:localhost,IP:127.0.0.1"},
        {"client", "orders.internal", "subjectAltName=DNS:orders.internal"},
      };
      for (String[] c : certs) {
        openssl(dir, "req", "-newkey", "rsa:2048", "-nodes", "-keyout", c[0] + ".key", "-out", c[0] + ".csr", "-subj",
            "/CN=" + c[1]);
        Files.writeString(dir.resolve(c[0] + ".ext"), c[2] + "\n");
        openssl(dir, "x509", "-req", "-in", c[0] + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
            "-days", "1", "-out", c[0] + ".pem", "-extfile", c[0] + ".ext");
      }
      // The same client key in PKCS#1 form, to check that format too.
      openssl(dir, "rsa", "-in", "client.key", "-out", "client-pkcs1.key", "-traditional");
      server = TestServer.start(new TestServer.Options(null, dir.resolve("server.pem"), dir.resolve("server.key"),
          dir.resolve("ca.pem")));
    }

    @AfterAll
    void stop() {
      if (server != null) {
        server.close();
      }
      TestServer.deleteTree(dir);
    }

    @Test
    void connectsWithAClientCertificate() {
      for (String key : new String[] {"client.key", "client-pkcs1.key"}) {
        TlsOptions tls = TlsOptions.builder().caPem(dir.resolve("ca.pem"))
            .clientCertificate(dir.resolve("client.pem"), dir.resolve(key)).build();
        try (ExspeedClient c = server.connect(b -> b.tls(tls))) {
          String s = uniq("mtls");
          c.createStream(s);
          assertEquals(0, c.publish(s, "mtls.ok", "mutual").offset());
        }
      }
    }

    @Test
    void isRefusedWithoutOne() {
      TlsOptions tls = TlsOptions.builder().caPem(dir.resolve("ca.pem")).build();
      assertThrows(ConnectionException.class, () -> server.connect(b -> b.tls(tls)
          .requestTimeout(Duration.ofSeconds(3))));
    }
  }
}
