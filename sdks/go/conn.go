package exspeed

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// connConfig is what one connection needs to know.
type connConfig struct {
	clientID       string
	token          *string
	tls            *tls.Config
	requestTimeout time.Duration
	keepalive      time.Duration
}

// connHandlers receive a connection's events.
type connHandlers struct {
	// onClose: the connection closed for any reason other than close().
	// Called once, on a goroutine of its own.
	onClose func(c *conn, err error)
	// onAsyncError: an Error with correlation id 0 (a fire-and-forget
	// request failed), or an undecodable push. Called on the read goroutine.
	onAsyncError func(err error)
}

// sink receives a subscription's pushes. Every method is called on the
// connection's read goroutine and must not block.
type sink interface {
	// onSubscribed is called when SubscribeOk arrives, before any push for
	// the subscription is routed.
	onSubscribed(c *conn, subID uint32)
	onDeliver(records []proto.WireRecord)
	onCoreMsg(m *proto.CoreMsg)
	onEnded(code uint16, message string)
}

type result struct {
	resp proto.Message
	err  error
}

// pending is a request waiting for its response: either a channel (for a
// blocking call) or a callback (for pipelined publishes).
type pending struct {
	corr  uint32
	ch    chan result
	cb    func(proto.Message, error)
	timer *time.Timer
	sink  sink
}

func (p *pending) complete(resp proto.Message, err error) {
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.cb != nil {
		p.cb(resp, err)
		return
	}
	p.ch <- result{resp, err}
}

// reqOpts tune one request.
type reqOpts struct {
	// extra time on top of the request timeout (a pull's or read's own wait).
	extra time.Duration
	// sink receives the pushes of a Subscribe / CoreSubscribe.
	sink sink
}

// conn is one TCP or TLS connection: handshake, frame reading,
// correlation-id multiplexing, push routing and keepalive. Reconnection
// lives one level up, in Client.
type conn struct {
	nc  net.Conn
	cfg *connConfig
	h   connHandlers

	wmu sync.Mutex // serializes frame writes

	mu         sync.Mutex
	pending    map[uint32]*pending
	subs       map[uint32]sink
	nextCorr   uint32
	closed     bool
	userClosed bool

	done       chan struct{} // closed on teardown
	readerDone chan struct{}
	wg         sync.WaitGroup // read and keepalive goroutines
	info       ServerInfo
}

// openConn dials addr and runs the Connect handshake.
func openConn(ctx context.Context, addr string, cfg *connConfig, h connHandlers) (*conn, error) {
	dctx, cancel := context.WithCancel(ctx)
	if cfg.requestTimeout > 0 {
		dctx, cancel = context.WithTimeout(ctx, cfg.requestTimeout)
	}
	defer cancel()
	d := &net.Dialer{KeepAlive: 10 * time.Second}
	var nc net.Conn
	var err error
	if cfg.tls != nil {
		td := &tls.Dialer{NetDialer: d, Config: cfg.tls}
		nc, err = td.DialContext(dctx, "tcp", addr)
	} else {
		nc, err = d.DialContext(dctx, "tcp", addr)
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &ConnectionError{Msg: "connect to " + addr + " failed", Err: err}
	}
	c := &conn{
		nc:         nc,
		cfg:        cfg,
		h:          h,
		pending:    make(map[uint32]*pending),
		subs:       make(map[uint32]sink),
		nextCorr:   1,
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	c.wg.Add(1)
	go c.readLoop()
	resp, err := c.request(dctx, proto.Connect{ClientID: cfg.clientID, Token: cfg.token}, reqOpts{})
	if err != nil {
		c.close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &ConnectionError{Msg: "connect to " + addr + " timed out", Err: err}
		}
		return nil, err
	}
	ok, isOk := resp.(proto.ConnectOk)
	if !isOk {
		c.close()
		return nil, &ProtocolError{Msg: fmt.Sprintf("unexpected handshake reply %T", resp)}
	}
	c.info = ServerInfo{ServerVersion: ok.ServerVersion, NodeID: ok.NodeID}
	if ok.Leader != nil {
		c.info.Leader = *ok.Leader
	}
	if cfg.keepalive > 0 {
		c.wg.Add(1)
		go c.keepaliveLoop()
	}
	return c, nil
}

// closedError is the error of a request made after the connection closed.
// Call with c.mu held.
func (c *conn) closedError() error {
	if c.userClosed {
		return &ConnectionError{Msg: "connection closed", Err: ErrClosed}
	}
	return &ConnectionError{Msg: "connection closed"}
}

func (c *conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// register allocates a correlation id for p and records it.
func (c *conn) register(p *pending, timeout time.Duration, onTimeout func()) (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, c.closedError()
	}
	corr := c.nextCorr
	for {
		if c.nextCorr == 0xffffffff {
			c.nextCorr = 1
		} else {
			c.nextCorr++
		}
		if _, used := c.pending[corr]; !used {
			break
		}
		corr = c.nextCorr
	}
	p.corr = corr
	if onTimeout != nil && timeout > 0 {
		p.timer = time.AfterFunc(timeout, onTimeout)
	}
	c.pending[corr] = p
	return corr, nil
}

// forget removes the pending request p, reporting whether it was still
// waiting (false: the read goroutine already took it and will complete it).
func (c *conn) forget(p *pending) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending[p.corr] == p {
		delete(c.pending, p.corr)
		return true
	}
	return false
}

func (c *conn) take(corr uint32) *pending {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pending[corr]
	if p != nil {
		delete(c.pending, corr)
	}
	return p
}

func encodeRequest(m proto.Message, corr uint32) ([]byte, error) {
	frame, err := proto.EncodeFrameOf(m, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	binary.LittleEndian.PutUint32(frame[2:], corr)
	return frame, nil
}

// write sends one frame. A failed write tears the connection down.
func (c *conn) write(frame []byte) error {
	c.wmu.Lock()
	if c.cfg.requestTimeout > 0 {
		_ = c.nc.SetWriteDeadline(time.Now().Add(c.cfg.requestTimeout))
	}
	_, err := c.nc.Write(frame)
	c.wmu.Unlock()
	if err != nil {
		cerr := &ConnectionError{Msg: "write failed", Err: err}
		c.teardown(cerr)
		return cerr
	}
	return nil
}

// request sends m and waits for its response. An Error reply becomes a
// *ServerError.
func (c *conn) request(ctx context.Context, m proto.Message, o reqOpts) (proto.Message, error) {
	frame, err := encodeRequest(m, 0)
	if err != nil {
		return nil, err
	}
	p := &pending{ch: make(chan result, 1), sink: o.sink}
	corr, err := c.register(p, 0, nil)
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(frame[2:], corr)
	_ = c.write(frame) // on failure, teardown completes p with the error
	timeout := c.cfg.requestTimeout + o.extra
	var expired <-chan time.Time
	if c.cfg.requestTimeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		expired = t.C
	}
	select {
	case r := <-p.ch:
		return r.resp, r.err
	case <-ctx.Done():
		if !c.forget(p) {
			r := <-p.ch
			return r.resp, r.err
		}
		return nil, ctx.Err()
	case <-expired:
		if !c.forget(p) {
			r := <-p.ch
			return r.resp, r.err
		}
		return nil, &TimeoutError{Msg: fmt.Sprintf("%s timed out after %v", opName(m), timeout)}
	}
}

// requestAsync sends m and calls cb exactly once with the response (on the
// read goroutine), a timeout, or a connection error. The frame is written
// before requestAsync returns, so calls from one goroutine reach the server
// in call order.
func (c *conn) requestAsync(m proto.Message, cb func(proto.Message, error)) error {
	frame, err := encodeRequest(m, 0)
	if err != nil {
		return err
	}
	p := &pending{cb: cb}
	timeout := c.cfg.requestTimeout
	corr, err := c.register(p, timeout, func() {
		if c.forget(p) {
			cb(nil, &TimeoutError{Msg: fmt.Sprintf("%s timed out after %v", opName(m), timeout)})
		}
	})
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(frame[2:], corr)
	_ = c.write(frame)
	return nil
}

// send writes m with correlation id 0 (fire-and-forget: no reply on
// success; a failure arrives as an async error).
func (c *conn) send(m proto.Message) error {
	c.mu.Lock()
	if c.closed {
		err := c.closedError()
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	frame, err := encodeRequest(m, 0)
	if err != nil {
		return err
	}
	return c.write(frame)
}

func (c *conn) addSub(id uint32, s sink) {
	c.mu.Lock()
	if !c.closed {
		c.subs[id] = s
	}
	c.mu.Unlock()
}

// removeSub stops routing pushes for a subscription.
func (c *conn) removeSub(id uint32) {
	c.mu.Lock()
	delete(c.subs, id)
	c.mu.Unlock()
}

func (c *conn) sub(id uint32) sink {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[id]
}

func (c *conn) readLoop() {
	defer c.wg.Done()
	defer close(c.readerDone)
	br := bufio.NewReaderSize(c.nc, 64*1024)
	for {
		f, err := proto.ReadFrame(br)
		if err != nil {
			var de *proto.DecodeError
			switch {
			case errors.As(err, &de):
				err = &ProtocolError{Msg: de.Msg}
				_ = c.nc.Close()
			case errors.Is(err, io.EOF):
				err = &ConnectionError{Msg: "connection closed by server"}
			default:
				err = &ConnectionError{Msg: "connection lost", Err: err}
			}
			c.teardown(err)
			return
		}
		resp, err := proto.DecodeResponse(f.Opcode, f.Payload)
		if err != nil {
			perr := &ProtocolError{Msg: err.Error()}
			if f.CorrelationID != 0 {
				if p := c.take(f.CorrelationID); p != nil {
					p.complete(nil, perr)
					continue
				}
			}
			if c.h.onAsyncError != nil {
				c.h.onAsyncError(perr)
			}
			continue
		}
		c.route(f.CorrelationID, resp)
	}
}

func serverError(e proto.Error) *ServerError {
	se := &ServerError{Code: int(e.Code), Message: e.Message}
	if len(e.Detail) > 0 {
		se.Detail = append([]byte(nil), e.Detail...)
	}
	return se
}

func (c *conn) route(corr uint32, resp proto.Message) {
	if corr == 0 {
		switch m := resp.(type) {
		case proto.Deliver:
			if s := c.sub(m.SubID); s != nil {
				s.onDeliver(m.Records)
			}
		case proto.CoreMsg:
			if s := c.sub(m.SubID); s != nil {
				s.onCoreMsg(&m)
			}
		case proto.SubscriptionEnded:
			c.mu.Lock()
			s := c.subs[m.SubID]
			delete(c.subs, m.SubID)
			c.mu.Unlock()
			if s != nil {
				s.onEnded(m.Code, m.Message)
			}
		case proto.Error:
			if c.h.onAsyncError != nil {
				c.h.onAsyncError(serverError(m))
			}
		}
		return
	}
	p := c.take(corr)
	if ok, isSub := resp.(proto.SubscribeOk); isSub {
		if p != nil && p.sink != nil {
			// Register before completing, so a Deliver in the next frame
			// is not lost.
			c.addSub(ok.SubID, p.sink)
			p.sink.onSubscribed(c, ok.SubID)
		} else {
			// The subscribe call was abandoned; release the server side.
			_ = c.send(proto.Unsubscribe{SubID: ok.SubID})
		}
	}
	if p == nil {
		return
	}
	if e, isErr := resp.(proto.Error); isErr {
		p.complete(nil, serverError(e))
	} else {
		p.complete(resp, nil)
	}
}

func (c *conn) keepaliveLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.keepalive)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		_, err := c.request(context.Background(), proto.Ping{}, reqOpts{})
		var te *TimeoutError
		if errors.As(err, &te) {
			// A ping that times out means the peer is gone (half-open socket).
			_ = c.nc.Close()
		}
	}
}

// teardown closes the connection and fails everything pending. Idempotent.
func (c *conn) teardown(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	user := c.userClosed
	pend := c.pending
	c.pending = make(map[uint32]*pending)
	c.subs = make(map[uint32]sink)
	c.mu.Unlock()

	close(c.done)
	_ = c.nc.Close()
	var cerr *ConnectionError
	switch {
	case user:
		cerr = &ConnectionError{Msg: "connection closed", Err: ErrClosed}
	case !errors.As(err, &cerr):
		cerr = &ConnectionError{Msg: "connection lost", Err: err}
	}
	for _, p := range pend {
		p.complete(nil, cerr)
	}
	if !user && c.h.onClose != nil {
		// On its own goroutine: teardown can run on a goroutine that holds
		// client locks (a failed write).
		go c.h.onClose(c, err)
	}
}

// close shuts the connection down gracefully: half-close, wait (at most a
// second) for the server to close its side, then release everything. It
// waits for the connection's goroutines; never call it from one of them.
func (c *conn) close() {
	c.mu.Lock()
	if !c.closed {
		c.userClosed = true
	}
	closed := c.closed
	c.mu.Unlock()
	if !closed {
		if cw, ok := c.nc.(interface{ CloseWrite() error }); ok {
			c.wmu.Lock()
			_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
			err := cw.CloseWrite()
			c.wmu.Unlock()
			if err == nil {
				_ = c.nc.SetReadDeadline(time.Now().Add(time.Second))
				<-c.readerDone
			}
		}
		c.teardown(&ConnectionError{Msg: "connection closed", Err: ErrClosed})
	}
	c.wg.Wait()
}

// opName names a request for error messages.
func opName(m proto.Message) string {
	name := fmt.Sprintf("%T", m)
	if i := len("proto."); len(name) > i && name[:i] == "proto." {
		return name[i:]
	}
	return name
}
