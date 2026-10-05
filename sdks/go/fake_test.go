package exspeed

import (
	"bufio"
	"context"
	"errors"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// received is one request the fake server got.
type received struct {
	corr uint32
	req  proto.Message
}

// fakeConn is one accepted client connection on the fake server.
type fakeConn struct {
	srv *fakeServer
	nc  net.Conn
	wmu sync.Mutex

	mu   sync.Mutex
	reqs []received
}

func (fc *fakeConn) reply(corr uint32, m proto.Message) {
	fc.replyMany(frame{corr, m})
}

type frame struct {
	corr uint32
	m    proto.Message
}

// replyMany writes several frames in a single write.
func (fc *fakeConn) replyMany(frames ...frame) {
	var buf []byte
	for _, f := range frames {
		b, err := proto.EncodeFrameOf(f.m, f.corr)
		if err != nil {
			panic(err)
		}
		buf = append(buf, b...)
	}
	fc.wmu.Lock()
	defer fc.wmu.Unlock()
	_, _ = fc.nc.Write(buf)
}

func (fc *fakeConn) received() []received {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]received(nil), fc.reqs...)
}

// of returns the requests of the same type as sample.
func (fc *fakeConn) of(sample proto.Message) []received {
	var out []received
	for _, r := range fc.received() {
		if reflect.TypeOf(r.req) == reflect.TypeOf(sample) {
			out = append(out, r)
		}
	}
	return out
}

// types lists the type names of the requests received, skipping the
// handshake.
func (fc *fakeConn) types() []string {
	var out []string
	for _, r := range fc.received() {
		if _, ok := r.req.(proto.Connect); ok {
			continue
		}
		out = append(out, opName(r.req))
	}
	return out
}

func (fc *fakeConn) drop() { _ = fc.nc.Close() }

// handlerFunc scripts the fake server. It returns true when it handled the
// request; Connect and Ping are answered automatically otherwise.
type handlerFunc func(fc *fakeConn, corr uint32, req proto.Message) bool

// fakeServer is a scriptable protocol-v2 server for unit tests.
type fakeServer struct {
	t  *testing.T
	ln net.Listener
	wg sync.WaitGroup

	mu      sync.Mutex
	conns   []*fakeConn
	handler handlerFunc
	notify  chan struct{}
	closed  bool
}

func startFake(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{t: t, ln: ln, notify: make(chan struct{})}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) setHandler(h handlerFunc) {
	s.mu.Lock()
	s.handler = h
	s.mu.Unlock()
}

func (s *fakeServer) getHandler() handlerFunc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handler
}

func (s *fakeServer) wakeWaiters() {
	s.mu.Lock()
	close(s.notify)
	s.notify = make(chan struct{})
	s.mu.Unlock()
}

func (s *fakeServer) accept() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		fc := &fakeConn{srv: s, nc: nc}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = nc.Close()
			return
		}
		s.conns = append(s.conns, fc)
		s.mu.Unlock()
		s.wakeWaiters()
		s.wg.Add(1)
		go s.serve(fc)
	}
}

func (s *fakeServer) serve(fc *fakeConn) {
	defer s.wg.Done()
	defer fc.nc.Close() // close on EOF, like the real server
	br := bufio.NewReader(fc.nc)
	for {
		f, err := proto.ReadFrame(br)
		if err != nil {
			return
		}
		req, err := proto.DecodeRequest(f.Opcode, f.Payload)
		if err != nil {
			fc.reply(f.CorrelationID, proto.Error{Code: 400, Message: err.Error()})
			continue
		}
		fc.mu.Lock()
		fc.reqs = append(fc.reqs, received{f.CorrelationID, req})
		fc.mu.Unlock()
		h := s.getHandler()
		if h == nil || !h(fc, f.CorrelationID, req) {
			switch req.(type) {
			case proto.Connect:
				fc.reply(f.CorrelationID, proto.ConnectOk{ServerVersion: "test", NodeID: "n1"})
			case proto.Ping:
				fc.reply(f.CorrelationID, proto.Pong{})
			}
		}
		s.wakeWaiters()
	}
}

func (s *fakeServer) connections() []*fakeConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*fakeConn(nil), s.conns...)
}

func (s *fakeServer) last() *fakeConn {
	c := s.connections()
	if len(c) == 0 {
		s.t.Fatal("no connection")
	}
	return c[len(c)-1]
}

// until waits for cond, re-checking whenever a request or connection
// arrives (and every 10 ms).
func (s *fakeServer) until(cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatal("fakeServer.until: timed out")
		}
		s.mu.Lock()
		ch := s.notify
		s.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *fakeServer) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	conns := s.conns
	s.mu.Unlock()
	_ = s.ln.Close()
	for _, c := range conns {
		_ = c.nc.Close()
	}
	s.wg.Wait()
}

// ---- helpers ----

// checkLeaks fails the test if goroutines of this package outlive it. Call
// it first, so its cleanup runs after the client's and the server's.
func checkLeaks(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			leaked := leakedGoroutines()
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("leaked goroutines:\n%s", strings.Join(leaked, "\n\n"))
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func leakedGoroutines() []string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "exspeed/sdks/go.") && !strings.Contains(g, "_test.go") {
			out = append(out, g)
		}
	}
	return out
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// connectFake connects to s with keepalive and reconnection off (unless
// opts say otherwise) and closes the client at the end of the test.
func connectFake(t *testing.T, s *fakeServer, opts ...Option) *Client {
	t.Helper()
	all := append([]Option{WithKeepalive(0), WithoutReconnect()}, opts...)
	c, err := Connect(testCtx(t), s.addr(), all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func wrec(offset uint64, value string) proto.WireRecord {
	return proto.WireRecord{
		Offset:        offset,
		TimestampNs:   1_700_000_000_000_000_000 + offset,
		DeliveryCount: 1,
		Subject:       "orders.placed",
		Value:         []byte(value),
		Headers:       []proto.Header{{Key: "h", Value: "1"}},
	}
}

func sp(s string) *string { return &s }

func mustCode(t *testing.T, err error, code int) *ServerError {
	t.Helper()
	var se *ServerError
	if !errors.As(err, &se) || se.Code != code {
		t.Fatalf("want ServerError %d, got %v", code, err)
	}
	return se
}

func eq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\n got: %#v\nwant: %#v", got, want)
	}
}
