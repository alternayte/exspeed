package exspeed

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"
)

// Version is this client library's version.
const Version = "0.7.0"

// ReconnectPolicy configures automatic reconnection.
type ReconnectPolicy struct {
	// MaxAttempts gives up after this many failed attempts in a row;
	// 0 = unlimited.
	MaxAttempts int
	// InitialDelay is the delay before the first attempt; it doubles each
	// attempt (with jitter). Default 100ms.
	InitialDelay time.Duration
	// MaxDelay bounds the delay between attempts. Default 5s.
	MaxDelay time.Duration
}

type clientConfig struct {
	addr           string
	servers        []string
	token          *string
	clientID       string
	tls            *tls.Config
	requestTimeout time.Duration
	keepalive      time.Duration
	reconnect      *ReconnectPolicy

	onDisconnect func(error)
	onReconnect  func(ServerInfo)
	onClose      func(error)
	onError      func(error)

	// ackLinger is how long a fire-and-forget ack may wait to share a frame
	// with later ones (also flushed before any other request, and whenever
	// a subscription runs out of buffered messages).
	ackLinger time.Duration
	// ackFlushAt flushes queued acks at once when this many are queued.
	ackFlushAt int
}

func defaultConfig() *clientConfig {
	return &clientConfig{
		addr:           fmt.Sprintf("127.0.0.1:%d", DefaultPort),
		clientID:       "exspeed-go",
		requestTimeout: 30 * time.Second,
		keepalive:      20 * time.Second,
		reconnect:      &ReconnectPolicy{InitialDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second},
		ackLinger:      5 * time.Millisecond,
		ackFlushAt:     256,
	}
}

func (c *clientConfig) connConfig() *connConfig {
	return &connConfig{
		clientID:       c.clientID,
		token:          c.token,
		tls:            c.tls,
		requestTimeout: c.requestTimeout,
		keepalive:      c.keepalive,
	}
}

// Option configures [Connect].
type Option func(*clientConfig)

// WithServers connects to whichever of these cluster nodes ("host:port")
// is the leader, following the leader hints followers return, and finds
// the new leader again after a failover. It overrides the address given
// to Connect.
func WithServers(addrs ...string) Option {
	return func(c *clientConfig) { c.servers = append([]string(nil), addrs...) }
}

// WithToken sets the bearer token, when the server runs with auth.
func WithToken(token string) Option {
	return func(c *clientConfig) { c.token = &token }
}

// WithClientID sets the id sent in the handshake and shown in server logs.
// Default "exspeed-go".
func WithClientID(id string) Option {
	return func(c *clientConfig) { c.clientID = id }
}

// WithTLS connects over TLS. A nil config verifies the server against the
// system roots. Set RootCAs for a private CA, and Certificates for a client
// certificate (mutual TLS); [LoadTLSConfig] builds such a config from PEM
// files. ServerName defaults to the host being dialed.
func WithTLS(cfg *tls.Config) Option {
	return func(c *clientConfig) {
		if cfg == nil {
			cfg = &tls.Config{}
		}
		c.tls = cfg
	}
}

// WithRequestTimeout sets how long to wait for a response (on top of a
// pull's or read's own wait). It also bounds connecting. Default 30s.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *clientConfig) { c.requestTimeout = d }
}

// WithKeepalive sets the ping interval; the server drops connections idle
// for 120s. 0 disables pings. Default 20s.
func WithKeepalive(d time.Duration) Option {
	return func(c *clientConfig) { c.keepalive = d }
}

// WithReconnect sets the reconnection policy (reconnection is on by
// default, with unlimited attempts from 100ms doubling to 5s).
func WithReconnect(p ReconnectPolicy) Option {
	return func(c *clientConfig) {
		if p.InitialDelay <= 0 {
			p.InitialDelay = 100 * time.Millisecond
		}
		if p.MaxDelay <= 0 {
			p.MaxDelay = 5 * time.Second
		}
		c.reconnect = &p
	}
}

// WithoutReconnect turns reconnection off: the client closes when the
// connection drops.
func WithoutReconnect() Option {
	return func(c *clientConfig) { c.reconnect = nil }
}

// WithDisconnectHandler is called when the connection drops and the client
// starts reconnecting.
func WithDisconnectHandler(f func(err error)) Option {
	return func(c *clientConfig) { c.onDisconnect = f }
}

// WithReconnectHandler is called once the client has reconnected and
// restored its subscriptions.
func WithReconnectHandler(f func(info ServerInfo)) Option {
	return func(c *clientConfig) { c.onReconnect = f }
}

// WithCloseHandler is called once when the client closes for good: err is
// nil after Close, or the last connection error when the connection dropped
// and reconnection was off or gave up.
func WithCloseHandler(f func(err error)) Option {
	return func(c *clientConfig) { c.onClose = f }
}

// WithErrorHandler receives errors that have no caller to return to: a
// fire-and-forget request (an ack or a credit) the server rejected, as a
// *ServerError, or a push this client could not decode, as a
// *ProtocolError.
func WithErrorHandler(f func(err error)) Option {
	return func(c *clientConfig) { c.onError = f }
}

// LoadTLSConfig builds a TLS config from PEM files. caFile (the CA that
// signed the server's certificate) may be "" to use the system roots;
// certFile and keyFile (a client certificate, for mutual TLS) may both be
// "".
func LoadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("exspeed: no certificates found in " + caFile)
		}
		cfg.RootCAs = pool
	}
	if certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
