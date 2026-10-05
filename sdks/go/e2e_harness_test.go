package exspeed_test

// End-to-end test harness: starts real exspeed servers.
//
// The binary comes from EXSPEED_BIN, or else target/debug/exspeed at the
// repository root (cargo build -p exspeed --bin exspeed). When neither
// exists the e2e tests are skipped with a message.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	exspeed "github.com/alternayte/exspeed/sdks/go"
)

var defaultBin, _ = filepath.Abs(filepath.Join("..", "..", "target", "debug", "exspeed"))

// serverBin is the server binary, or "" when there is none.
func serverBin() string {
	if p := os.Getenv("EXSPEED_BIN"); p != "" {
		abs, err := filepath.Abs(p)
		if err != nil {
			return p
		}
		return abs
	}
	if _, err := os.Stat(defaultBin); err == nil {
		return defaultBin
	}
	return ""
}

func skipMessage() string {
	return "e2e tests skipped: set EXSPEED_BIN to an exspeed server binary, or build one with " +
		"`cargo build -p exspeed --bin exspeed` (looked for " + defaultBin + ")"
}

func TestMain(m *testing.M) {
	if bin := serverBin(); bin == "" {
		fmt.Fprintf(os.Stderr, "\n[exspeed-go] %s\n\n", skipMessage())
	} else if _, err := os.Stat(bin); err != nil {
		fmt.Fprintf(os.Stderr, "EXSPEED_BIN=%s does not exist\n", os.Getenv("EXSPEED_BIN"))
		os.Exit(1)
	}
	code := m.Run()
	stopShared()
	os.Exit(code)
}

type serverOpts struct {
	authToken   string
	tlsCert     string
	tlsKey      string
	tlsClientCA string
}

// syncBuffer keeps the tail of the server's log.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len() > 1<<20 {
		b.buf.Reset()
	}
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type testServer struct {
	port, apiPort int
	dataDir       string
	opts          serverOpts
	cmd           *exec.Cmd
	exited        chan struct{}
	log           *syncBuffer
}

func freePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startServer starts a server for one test (stopped at its end), or skips
// the test when there is no binary.
func startServer(t *testing.T, opts serverOpts) *testServer {
	t.Helper()
	if serverBin() == "" {
		t.Skip(skipMessage())
	}
	s := &testServer{port: freePort(t), apiPort: freePort(t), dataDir: t.TempDir(), opts: opts}
	if err := s.spawn(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.kill)
	return s
}

func (s *testServer) spawn() error {
	args := []string{"server", "--bind", fmt.Sprintf("127.0.0.1:%d", s.port),
		"--api-bind", fmt.Sprintf("127.0.0.1:%d", s.apiPort), "--data-dir", s.dataDir}
	if s.opts.authToken != "" {
		args = append(args, "--auth-token", s.opts.authToken)
	}
	if s.opts.tlsCert != "" {
		args = append(args, "--tls-cert", s.opts.tlsCert, "--tls-key", s.opts.tlsKey)
	}
	if s.opts.tlsClientCA != "" {
		args = append(args, "--tls-client-ca", s.opts.tlsClientCA)
	}
	cmd := exec.Command(serverBin(), args...)
	// Don't let the developer's environment change the server under test.
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "EXSPEED_") && !strings.HasPrefix(kv, "RUST_LOG=") {
			env = append(env, kv)
		}
	}
	cmd.Env = append(env, "RUST_LOG=warn")
	s.log = &syncBuffer{}
	cmd.Stdout, cmd.Stderr = s.log, s.log
	if err := cmd.Start(); err != nil {
		return err
	}
	s.cmd = cmd
	s.exited = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(s.exited)
	}()
	if err := s.waitReady(30 * time.Second); err != nil {
		s.kill()
		return fmt.Errorf("%v\n--- server log ---\n%s", err, s.log.String())
	}
	return nil
}

func (s *testServer) waitReady(timeout time.Duration) error {
	scheme := "http"
	if s.opts.tlsCert != "" {
		scheme = "https"
	}
	hc := &http.Client{
		Timeout: time.Second,
		// Readiness only; the client under test verifies certificates.
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec
	}
	defer hc.CloseIdleConnections()
	url := fmt.Sprintf("%s://127.0.0.1:%d/readyz", scheme, s.apiPort)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-s.exited:
			return fmt.Errorf("server exited: %v", s.cmd.ProcessState)
		default:
		}
		if resp, err := hc.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("server did not become ready")
}

// kill stops the process (SIGTERM, then SIGKILL after 5 s).
func (s *testServer) kill() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	select {
	case <-s.exited:
		return
	default:
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.exited
	}
}

// restart stops the process and starts a new one on the same ports and
// data directory.
func (s *testServer) restart(t *testing.T) {
	t.Helper()
	s.kill()
	if err := s.spawn(); err != nil {
		t.Fatal(err)
	}
}

func (s *testServer) addr() string { return fmt.Sprintf("127.0.0.1:%d", s.port) }

// connect connects a client (reconnection off unless opts turn it on) and
// closes it at the end of the test.
func (s *testServer) connect(t *testing.T, opts ...exspeed.Option) *exspeed.Client {
	t.Helper()
	c, err := s.tryConnect(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (s *testServer) tryConnect(opts ...exspeed.Option) (*exspeed.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return exspeed.Connect(ctx, s.addr(), append([]exspeed.Option{exspeed.WithoutReconnect()}, opts...)...)
}

// ---- the shared server ----

var (
	sharedMu  sync.Mutex
	sharedSrv *testServer
)

// shared is one server used by the tests that need nothing special
// (stopped by TestMain).
func shared(t *testing.T) *testServer {
	t.Helper()
	if serverBin() == "" {
		t.Skip(skipMessage())
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedSrv == nil {
		dir, err := os.MkdirTemp("", "exspeed-go-e2e-")
		if err != nil {
			t.Fatal(err)
		}
		s := &testServer{port: freePort(t), apiPort: freePort(t), dataDir: dir}
		if err := s.spawn(); err != nil {
			os.RemoveAll(dir)
			t.Fatal(err)
		}
		sharedSrv = s
	}
	return sharedSrv
}

func stopShared() {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedSrv != nil {
		sharedSrv.kill()
		os.RemoveAll(sharedSrv.dataDir)
		sharedSrv = nil
	}
}

// ---- helpers ----

var counter atomic.Int64

// uniq is a unique, valid stream or consumer name.
func uniq(prefix string) string {
	return fmt.Sprintf("%s-%d-%s-%d", prefix, os.Getpid(), strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()%1e9)), counter.Add(1))
}

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// eventually polls f until it returns true.
func eventually(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("eventually: timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// certs is a CA plus server and client certificates signed by it, as PEM
// files in a temp dir.
type certs struct {
	caFile, serverCert, serverKey, clientCert, clientKey string
}

func makeCerts(t *testing.T) certs {
	t.Helper()
	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	newKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	serial := int64(1)
	tmpl := func(cn string) *x509.Certificate {
		serial++
		return &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
		}
	}
	caKey := newKey()
	caT := tmpl("exspeed-test-ca")
	caT.IsCA, caT.BasicConstraintsValid = true, true
	caT.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	caDER, err := x509.CreateCertificate(rand.Reader, caT, caT, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	leaf := func(cn string, dns []string, ips []net.IP, usage x509.ExtKeyUsage, name string) (string, string) {
		k := newKey()
		lt := tmpl(cn)
		lt.DNSNames, lt.IPAddresses = dns, ips
		lt.KeyUsage = x509.KeyUsageDigitalSignature
		lt.ExtKeyUsage = []x509.ExtKeyUsage{usage}
		der, err := x509.CreateCertificate(rand.Reader, lt, caCert, &k.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kder, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		return write(name+".pem", "CERTIFICATE", der), write(name+".key", "PRIVATE KEY", kder)
	}
	c := certs{caFile: write("ca.pem", "CERTIFICATE", caDER)}
	c.serverCert, c.serverKey = leaf("localhost", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, x509.ExtKeyUsageServerAuth, "server")
	c.clientCert, c.clientKey = leaf("orders.internal", []string{"orders.internal"}, nil, x509.ExtKeyUsageClientAuth, "client")
	return c
}
