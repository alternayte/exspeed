package exspeed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// inboxPrefix starts the subjects of request-reply inboxes.
const inboxPrefix = "_INBOX"

// CoreMessage is a core (non-persistent) message: from a core
// subscription, or the response to a [Client.Request].
type CoreMessage struct {
	Subject string
	// ReplyTo is set on a request: answer it with [CoreMessage.Respond].
	ReplyTo string
	// Headers in wire order; a key can repeat.
	Headers []Header
	Value   []byte

	c *Client
}

func newCoreMessage(c *Client, m *proto.CoreMsg) *CoreMessage {
	cm := &CoreMessage{Subject: m.Subject, Headers: fromWireHeaders(m.Headers), Value: m.Value, c: c}
	if m.ReplyTo != nil {
		cm.ReplyTo = *m.ReplyTo
	}
	return cm
}

// Text is the value as a string.
func (m *CoreMessage) Text() string { return string(m.Value) }

// JSON unmarshals the value into v.
func (m *CoreMessage) JSON(v any) error { return json.Unmarshal(m.Value, v) }

// Header is the value of the first header named name.
func (m *CoreMessage) Header(name string) (string, bool) { return findHeader(m.Headers, name) }

// Respond answers a request: it publishes value to the message's ReplyTo.
func (m *CoreMessage) Respond(ctx context.Context, value []byte, headers ...Header) error {
	if m.ReplyTo == "" {
		return invalidf("message has no reply subject")
	}
	return m.c.PublishCore(ctx, m.ReplyTo, value, headers...)
}

// PublishCore publishes a core message to the core subscriptions live now.
// Nothing is stored and delivery is at most once. It returns once the
// server accepted the message.
func (c *Client) PublishCore(ctx context.Context, subject string, value []byte, headers ...Header) error {
	return c.publishCore(ctx, subject, nil, value, headers)
}

// PublishCoreWithReply publishes a core message that asks receivers to
// answer on replyTo. It fails with 404 ([ErrNotFound]) when nobody
// received it. [Client.Request] does this for you.
func (c *Client) PublishCoreWithReply(ctx context.Context, subject, replyTo string, value []byte, headers ...Header) error {
	return c.publishCore(ctx, subject, &replyTo, value, headers)
}

func (c *Client) publishCore(ctx context.Context, subject string, replyTo *string, value []byte, headers []Header) error {
	if value == nil {
		value = []byte{}
	}
	return c.callOk(ctx, proto.CorePublish{Subject: subject, ReplyTo: replyTo, Headers: toWireHeaders(headers), Value: value})
}

// SubscribeCore receives core messages on subjects matching subject (a
// filter such as orders.*).
func (c *Client) SubscribeCore(ctx context.Context, subject string) (*CoreSubscription, error) {
	return c.subscribeCore(ctx, subject, "")
}

// QueueSubscribeCore is SubscribeCore in a queue group: each message goes
// to one member of the group.
func (c *Client) QueueSubscribeCore(ctx context.Context, subject, queue string) (*CoreSubscription, error) {
	if queue == "" {
		return nil, invalidf("queue group name is required")
	}
	return c.subscribeCore(ctx, subject, queue)
}

func (c *Client) subscribeCore(ctx context.Context, subject, queue string) (*CoreSubscription, error) {
	s := &CoreSubscription{c: c, subject: subject, queue: queue, wake: newSignal(), stopped: make(chan struct{})}
	if _, err := call[proto.SubscribeOk](ctx, c, proto.CoreSubscribe{Subject: subject, Queue: s.queuePtr()}, reqOpts{sink: s}); err != nil {
		s.end(0, "subscribe failed", false)
		return nil, err
	}
	c.mu.Lock()
	if c.state == stateClosed {
		c.mu.Unlock()
		s.end(0, "client closed", false)
		return s, nil
	}
	c.coreSubs[s] = struct{}{}
	c.mu.Unlock()
	return s, nil
}

func (c *Client) forgetCoreSub(s *CoreSubscription) {
	c.mu.Lock()
	delete(c.coreSubs, s)
	c.mu.Unlock()
}

// CoreSubscription is a core-message subscription. Take messages with
// [CoreSubscription.Next] or from [CoreSubscription.Messages]. It ends on
// Unsubscribe, when the server ends it (503 when leadership moves), or when
// the client closes.
//
// Core messages are not stored: a subscription receives what is published
// while it is live. After a reconnect the client subscribes again with the
// same subject and queue group; messages published in the gap are missed.
type CoreSubscription struct {
	c       *Client
	subject string
	queue   string

	mu    sync.Mutex
	cn    *conn
	subID uint32
	buf   []*CoreMessage
	head  int
	wake  signal
	ended *SubscriptionEndedError

	stopOnce sync.Once
	stopped  chan struct{}
	pumpOnce sync.Once
	msgs     chan *CoreMessage
}

func (s *CoreSubscription) queuePtr() *string {
	if s.queue == "" {
		return nil
	}
	q := s.queue
	return &q
}

// Subject is the subscription's subject filter.
func (s *CoreSubscription) Subject() string { return s.subject }

// Queue is the queue group, or "".
func (s *CoreSubscription) Queue() string { return s.queue }

// ID is the server-assigned id of the current subscription (high bit set;
// it changes after a reconnect).
func (s *CoreSubscription) ID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subID
}

// Buffered is the number of messages received but not yet taken.
func (s *CoreSubscription) Buffered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf) - s.head
}

// Closed reports whether the subscription has ended.
func (s *CoreSubscription) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended != nil
}

// EndReason says why the subscription ended, or nil while it is active.
func (s *CoreSubscription) EndReason() *SubscriptionEndedError {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended == nil {
		return nil
	}
	e := *s.ended
	return &e
}

// Next returns the next message, waiting as long as ctx allows. Once the
// subscription has ended and its buffer is drained, it returns a
// [*SubscriptionEndedError]. When ctx is done first it returns ctx.Err().
func (s *CoreSubscription) Next(ctx context.Context) (*CoreMessage, error) {
	for {
		s.mu.Lock()
		if s.head < len(s.buf) {
			m := s.buf[s.head]
			s.buf[s.head] = nil
			s.head++
			if s.head == len(s.buf) {
				s.buf, s.head = s.buf[:0], 0
			} else if s.head > 1024 && s.head*2 > len(s.buf) {
				n := copy(s.buf, s.buf[s.head:])
				s.buf, s.head = s.buf[:n], 0
			}
			s.mu.Unlock()
			return m, nil
		}
		if s.ended != nil {
			e := *s.ended
			s.mu.Unlock()
			return nil, &e
		}
		wake := s.wake.ch
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Messages returns a channel that delivers the subscription's messages and
// is closed when it ends. The first call starts a goroutine that feeds the
// channel; it exits when the subscription ends. Use either Messages or
// Next, not both.
func (s *CoreSubscription) Messages() <-chan *CoreMessage {
	s.pumpOnce.Do(func() {
		s.msgs = make(chan *CoreMessage)
		go s.pump()
	})
	return s.msgs
}

func (s *CoreSubscription) pump() {
	defer close(s.msgs)
	for {
		m, err := s.Next(s.c.ctx)
		if err != nil {
			return
		}
		select {
		case s.msgs <- m:
		case <-s.stopped:
			return
		case <-s.c.ctx.Done():
			return
		}
	}
}

// Unsubscribe stops delivery. Buffered messages are dropped.
func (s *CoreSubscription) Unsubscribe(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	s.mu.Lock()
	if s.ended != nil {
		s.mu.Unlock()
		return nil
	}
	cn, id := s.cn, s.subID
	s.mu.Unlock()
	s.end(0, "unsubscribed", false)
	if cn != nil && !cn.isClosed() {
		cn.removeSub(id)
		if _, err := cn.request(ctx, proto.Unsubscribe{SubID: id}, reqOpts{}); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (s *CoreSubscription) onSubscribed(cn *conn, id uint32) {
	s.mu.Lock()
	if s.ended != nil {
		s.mu.Unlock()
		cn.removeSub(id)
		_ = cn.send(proto.Unsubscribe{SubID: id})
		return
	}
	s.cn, s.subID = cn, id
	s.mu.Unlock()
}

func (s *CoreSubscription) onDeliver([]proto.WireRecord) {}

func (s *CoreSubscription) onCoreMsg(m *proto.CoreMsg) {
	msg := newCoreMessage(s.c, m)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended != nil {
		return
	}
	s.buf = append(s.buf, msg)
	s.wake.fire()
}

// onEnded is a server-side end: buffered messages are still yielded.
func (s *CoreSubscription) onEnded(code uint16, message string) { s.end(int(code), message, true) }

// suspend: the connection was lost; a re-subscribe follows. Buffered
// messages are kept.
func (s *CoreSubscription) suspend() {
	s.mu.Lock()
	s.cn = nil
	s.mu.Unlock()
}

func (s *CoreSubscription) end(code int, message string, keepBuffered bool) {
	s.mu.Lock()
	if s.ended != nil {
		s.mu.Unlock()
		return
	}
	s.ended = &SubscriptionEndedError{Code: code, Message: message}
	s.cn = nil
	if !keepBuffered {
		s.buf, s.head = nil, 0
	}
	s.wake.fire()
	s.mu.Unlock()
	s.c.forgetCoreSub(s)
}

// ---- request-reply ------------------------------------------------------------

type replyResult struct {
	msg *CoreMessage
	err error
}

// inbox is the connection's request-reply inbox: one core subscription to
// <prefix>.*, set up by the first request.
type inbox struct {
	prefix string
	ready  chan struct{} // closed once the subscribe finished
	err    error         // its error, set before ready is closed
	cn     *conn
	subID  uint32
}

// Request sends a request (a core message with a reply subject) and returns
// the first response. It fails at once with 404 ([ErrNotFound]) when nobody
// is subscribed to subject, and with a [*TimeoutError] when no response
// arrives before ctx's deadline or the client's request timeout, whichever
// is first.
//
// All requests on a connection share one inbox subscription
// (_INBOX.<random>.*), set up by the first request.
func (c *Client) Request(ctx context.Context, subject string, value []byte, headers ...Header) (*CoreMessage, error) {
	timeout := c.cfg.requestTimeout
	if dl, ok := ctx.Deadline(); ok && (timeout <= 0 || time.Until(dl) < timeout) {
		timeout = time.Until(dl)
	}
	prefix, err := c.ensureInbox(ctx)
	if err != nil {
		return nil, timeoutFromCtx(ctx, err, subject, timeout)
	}
	ch := make(chan replyResult, 1)
	c.mu.Lock()
	c.nextReply++
	token := strconv.FormatUint(c.nextReply, 10)
	c.replies[token] = ch
	replies := c.replies
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(replies, token)
		c.mu.Unlock()
	}()
	if err := c.PublishCoreWithReply(ctx, subject, prefix+"."+token, value, headers...); err != nil {
		return nil, timeoutFromCtx(ctx, err, subject, timeout)
	}
	var expired <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		expired = t.C
	}
	select {
	case r := <-ch:
		return r.msg, r.err
	case <-expired:
		return nil, &TimeoutError{Msg: fmt.Sprintf("request to %s timed out after %v", subject, timeout)}
	case <-ctx.Done():
		return nil, timeoutFromCtx(ctx, ctx.Err(), subject, timeout)
	}
}

// timeoutFromCtx turns an expired deadline into a *TimeoutError.
func timeoutFromCtx(ctx context.Context, err error, subject string, timeout time.Duration) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && errors.Is(err, context.DeadlineExceeded) {
		return &TimeoutError{Msg: fmt.Sprintf("request to %s timed out after %v", subject, timeout)}
	}
	return err
}

// ensureInbox subscribes the connection's inbox if needed and returns its
// subject prefix.
func (c *Client) ensureInbox(ctx context.Context) (string, error) {
	c.mu.Lock()
	ib := c.inbox
	if ib == nil {
		switch c.state {
		case stateClosed:
			c.mu.Unlock()
			return "", &ConnectionError{Msg: "client is closed", Err: ErrClosed}
		case stateReconnecting:
			c.mu.Unlock()
			return "", &ConnectionError{Msg: "not connected (reconnecting)"}
		}
		var rnd [12]byte
		_, _ = rand.Read(rnd[:])
		ib = &inbox{prefix: inboxPrefix + "." + hex.EncodeToString(rnd[:]), ready: make(chan struct{})}
		c.inbox = ib
		cn := c.conn
		c.wg.Add(1)
		go c.subscribeInbox(cn, ib)
	}
	c.mu.Unlock()
	select {
	case <-ib.ready:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if ib.err != nil {
		return "", ib.err
	}
	return ib.prefix, nil
}

// subscribeInbox runs on its own goroutine, so a caller giving up doesn't
// fail the other requests waiting for the inbox.
func (c *Client) subscribeInbox(cn *conn, ib *inbox) {
	defer c.wg.Done()
	_, err := cn.request(c.ctx, proto.CoreSubscribe{Subject: ib.prefix + ".*"}, reqOpts{sink: &inboxSink{c: c, ib: ib}})
	if err != nil {
		c.mu.Lock()
		if c.inbox == ib {
			c.inbox = nil // the next request tries again
		}
		c.mu.Unlock()
		ib.err = err
	}
	close(ib.ready)
}

type inboxSink struct {
	c  *Client
	ib *inbox
}

func (s *inboxSink) onSubscribed(cn *conn, id uint32) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.c.inbox != s.ib {
		// Replaced (connection lost) while subscribing: release it.
		cn.removeSub(id)
		_ = cn.send(proto.Unsubscribe{SubID: id})
		return
	}
	s.ib.cn, s.ib.subID = cn, id
}

func (s *inboxSink) onDeliver([]proto.WireRecord) {}

func (s *inboxSink) onCoreMsg(m *proto.CoreMsg) {
	token := m.Subject[strings.LastIndexByte(m.Subject, '.')+1:]
	s.c.mu.Lock()
	ch := s.c.replies[token]
	delete(s.c.replies, token)
	s.c.mu.Unlock()
	if ch != nil {
		ch <- replyResult{msg: newCoreMessage(s.c, m)} // buffered; late or duplicate responses are dropped
	}
}

func (s *inboxSink) onEnded(code uint16, message string) {
	s.c.mu.Lock()
	if s.c.inbox != s.ib {
		s.c.mu.Unlock()
		return
	}
	waiters := s.c.dropInboxLocked()
	s.c.mu.Unlock()
	failReplies(waiters, &ServerError{Code: int(code), Message: message})
}

// dropInboxLocked forgets the inbox (the next request subscribes a new one)
// and returns the requests waiting on it. Call with c.mu held.
func (c *Client) dropInboxLocked() []chan replyResult {
	ib := c.inbox
	c.inbox = nil
	if ib != nil && ib.cn != nil {
		ib.cn.removeSub(ib.subID)
	}
	waiters := make([]chan replyResult, 0, len(c.replies))
	for _, ch := range c.replies {
		waiters = append(waiters, ch)
	}
	c.replies = make(map[string]chan replyResult)
	return waiters
}

func failReplies(waiters []chan replyResult, err error) {
	for _, ch := range waiters {
		select {
		case ch <- replyResult{err: err}:
		default:
		}
	}
}
