package io.exspeed.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS settings: which CAs to trust, an optional client certificate for mutual
 * TLS, and the server name to verify. Certificates are verified, hostname
 * included, by default.
 *
 * <pre>{@code
 * TlsOptions tls = TlsOptions.builder()
 *     .caPem(Path.of("ca.pem"))                                       // trust a private CA
 *     .clientCertificate(Path.of("client.pem"), Path.of("client.key")) // mutual TLS
 *     .build();
 * }</pre>
 *
 * <p>Certificates are read from PEM. Private keys are read from unencrypted
 * PEM: PKCS#8 ({@code BEGIN PRIVATE KEY}, RSA, EC or EdDSA), PKCS#1
 * ({@code BEGIN RSA PRIVATE KEY}) or SEC1 ({@code BEGIN EC PRIVATE KEY}). For
 * anything else, build an {@link SSLContext} yourself and pass it to
 * {@link Builder#sslContext(SSLContext)}.
 */
public final class TlsOptions {
  private final SSLContext context;
  private final String serverName;
  private final boolean verifyHostname;

  private TlsOptions(SSLContext context, String serverName, boolean verifyHostname) {
    this.context = context;
    this.serverName = serverName;
    this.verifyHostname = verifyHostname;
  }

  /**
   * TLS verified against the JVM's default trust store.
   *
   * @return the options
   */
  public static TlsOptions systemDefault() {
    return builder().build();
  }

  /**
   * Starts building TLS options.
   *
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The SSL context sockets are created from.
   *
   * @return the context
   */
  public SSLContext sslContext() {
    return context;
  }

  /**
   * The name sent as SNI and checked against the certificate.
   *
   * @return the name, or {@code null} to use the host connected to
   */
  public String serverName() {
    return serverName;
  }

  /**
   * Whether the certificate must match the host (or {@link #serverName()}).
   *
   * @return the setting
   */
  public boolean verifyHostname() {
    return verifyHostname;
  }

  /** Builds {@link TlsOptions}. */
  public static final class Builder {
    private SSLContext context;
    private final List<Certificate> cas = new ArrayList<>();
    private KeyStore trustStore;
    private List<Certificate> clientChain;
    private PrivateKey clientKey;
    private String serverName;
    private boolean verifyHostname = true;

    private Builder() {}

    /**
     * Trusts the CA certificates in a PEM file instead of the system's.
     *
     * @param pemFile the PEM file (one or more certificates)
     * @return this builder
     */
    public Builder caPem(Path pemFile) {
      return caPem(read(pemFile));
    }

    /**
     * Trusts the CA certificates in PEM text instead of the system's.
     *
     * @param pem the PEM text (one or more certificates)
     * @return this builder
     */
    public Builder caPem(String pem) {
      cas.addAll(parseCertificates(pem));
      return this;
    }

    /**
     * Trusts the certificates in a key store instead of the system's.
     *
     * @param trustStore the trust store
     * @return this builder
     */
    public Builder trustStore(KeyStore trustStore) {
      this.trustStore = trustStore;
      return this;
    }

    /**
     * Presents a client certificate (mutual TLS).
     *
     * @param certPemFile the certificate chain, PEM
     * @param keyPemFile the private key, unencrypted PEM
     * @return this builder
     */
    public Builder clientCertificate(Path certPemFile, Path keyPemFile) {
      return clientCertificate(read(certPemFile), read(keyPemFile));
    }

    /**
     * Presents a client certificate (mutual TLS).
     *
     * @param certPem the certificate chain, PEM text
     * @param keyPem the private key, unencrypted PEM text
     * @return this builder
     */
    public Builder clientCertificate(String certPem, String keyPem) {
      this.clientChain = parseCertificates(certPem);
      this.clientKey = parsePrivateKey(keyPem);
      return this;
    }

    /**
     * Uses a ready-made SSL context; the CA and client certificate settings
     * are ignored.
     *
     * @param context the context
     * @return this builder
     */
    public Builder sslContext(SSLContext context) {
      this.context = context;
      return this;
    }

    /**
     * Sends this name as SNI and verifies the certificate against it instead
     * of the host connected to.
     *
     * @param name the server name
     * @return this builder
     */
    public Builder serverName(String name) {
      this.serverName = name;
      return this;
    }

    /**
     * Whether the certificate must match the host name (default true). Turn
     * it off only for development.
     *
     * @param verify the setting
     * @return this builder
     */
    public Builder verifyHostname(boolean verify) {
      this.verifyHostname = verify;
      return this;
    }

    /**
     * Builds the options, creating the SSL context.
     *
     * @return the options
     * @throws ExspeedException when the context can't be created
     */
    public TlsOptions build() {
      SSLContext ctx = context;
      if (ctx == null) {
        try {
          TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          if (trustStore != null || !cas.isEmpty()) {
            KeyStore ts = trustStore;
            if (ts == null) {
              ts = KeyStore.getInstance(KeyStore.getDefaultType());
              ts.load(null, null);
            }
            int i = 0;
            for (Certificate c : cas) {
              ts.setCertificateEntry("exspeed-ca-" + i++, c);
            }
            tmf.init(ts);
          } else {
            tmf.init((KeyStore) null);
          }
          KeyManagerFactory kmf = null;
          if (clientKey != null) {
            char[] pw = new char[0];
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            ks.setKeyEntry("client", clientKey, pw, clientChain.toArray(new Certificate[0]));
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pw);
          }
          ctx = SSLContext.getInstance("TLS");
          ctx.init(kmf == null ? null : kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (GeneralSecurityException | IOException e) {
          throw new ExspeedException("cannot set up TLS: " + e.getMessage(), e);
        }
      }
      return new TlsOptions(ctx, serverName, verifyHostname);
    }
  }

  private static String read(Path p) {
    try {
      return Files.readString(p, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new ExspeedException("cannot read " + p + ": " + e.getMessage(), e);
    }
  }

  static List<Certificate> parseCertificates(String pem) {
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Collection<? extends Certificate> certs =
          cf.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)));
      if (certs.isEmpty()) {
        throw new ExspeedException("no certificate found in PEM");
      }
      return new ArrayList<>(certs);
    } catch (GeneralSecurityException e) {
      throw new ExspeedException("invalid certificate PEM: " + e.getMessage(), e);
    }
  }

  private static final Pattern PEM_BLOCK =
      Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----");

  static PrivateKey parsePrivateKey(String pem) {
    Matcher m = PEM_BLOCK.matcher(pem);
    while (m.find()) {
      String type = m.group(1);
      byte[] der = Base64.getMimeDecoder().decode(m.group(2));
      switch (type) {
        case "PRIVATE KEY":
          return pkcs8(der);
        case "RSA PRIVATE KEY":
          return pkcs8(wrapPkcs8(RSA_ALG, der));
        case "EC PRIVATE KEY":
          return pkcs8(wrapPkcs8(ecAlgorithm(der), der));
        case "ENCRYPTED PRIVATE KEY":
          throw new ExspeedException("encrypted private keys are not supported; decrypt it or pass an SSLContext");
        default:
          // Not a key (e.g. EC PARAMETERS): keep looking.
      }
    }
    throw new ExspeedException("no private key found in PEM");
  }

  private static PrivateKey pkcs8(byte[] der) {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
    for (String alg : new String[] {"RSA", "EC", "Ed25519", "Ed448", "RSASSA-PSS"}) {
      try {
        return KeyFactory.getInstance(alg).generatePrivate(spec);
      } catch (GeneralSecurityException e) {
        // try the next algorithm
      }
    }
    throw new ExspeedException("unsupported private key type");
  }

  // AlgorithmIdentifier { rsaEncryption, NULL }
  private static final byte[] RSA_ALG = {
    0x30, 0x0d, 0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00
  };
  // OID 1.2.840.10045.2.1 (id-ecPublicKey)
  private static final byte[] EC_PUBLIC_KEY_OID = {0x06, 0x07, 0x2a, (byte) 0x86, 0x48, (byte) 0xce, 0x3d, 0x02, 0x01};

  /** AlgorithmIdentifier { id-ecPublicKey, namedCurve } from a SEC1 key's [0] parameters. */
  private static byte[] ecAlgorithm(byte[] sec1) {
    int[] p = {0};
    expectTag(sec1, p, 0x30);
    readLength(sec1, p);
    expectTag(sec1, p, 0x02); // version
    p[0] += readLength(sec1, p);
    expectTag(sec1, p, 0x04); // private key
    p[0] += readLength(sec1, p);
    if (p[0] >= sec1.length || (sec1[p[0]] & 0xff) != 0xa0) {
      throw new ExspeedException("EC private key without curve parameters; use PKCS#8");
    }
    p[0]++;
    int len = readLength(sec1, p);
    byte[] curveOid = java.util.Arrays.copyOfRange(sec1, p[0], p[0] + len);
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.writeBytes(EC_PUBLIC_KEY_OID);
    body.writeBytes(curveOid);
    return der(0x30, body.toByteArray());
  }

  private static byte[] wrapPkcs8(byte[] algorithm, byte[] key) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.writeBytes(new byte[] {0x02, 0x01, 0x00});
    body.writeBytes(algorithm);
    body.writeBytes(der(0x04, key));
    return der(0x30, body.toByteArray());
  }

  private static byte[] der(int tag, byte[] content) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(tag);
    int n = content.length;
    if (n < 0x80) {
      out.write(n);
    } else if (n < 0x100) {
      out.write(0x81);
      out.write(n);
    } else if (n < 0x10000) {
      out.write(0x82);
      out.write(n >> 8);
      out.write(n);
    } else {
      out.write(0x83);
      out.write(n >> 16);
      out.write(n >> 8);
      out.write(n);
    }
    out.writeBytes(content);
    return out.toByteArray();
  }

  private static void expectTag(byte[] b, int[] p, int tag) {
    if (p[0] >= b.length || (b[p[0]] & 0xff) != tag) {
      throw new ExspeedException("malformed private key");
    }
    p[0]++;
  }

  private static int readLength(byte[] b, int[] p) {
    if (p[0] >= b.length) {
      throw new ExspeedException("malformed private key");
    }
    int first = b[p[0]++] & 0xff;
    if (first < 0x80) {
      return first;
    }
    int n = first & 0x7f;
    if (n > 3 || p[0] + n > b.length) {
      throw new ExspeedException("malformed private key");
    }
    int len = 0;
    for (int i = 0; i < n; i++) {
      len = (len << 8) | (b[p[0]++] & 0xff);
    }
    return len;
  }
}
