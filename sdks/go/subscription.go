package exspeed

import (
	"context"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// Message is a record delivered by a consumer (push or pull), with the
// means to settle it.
type Message struct {
	Record
	// DeliveryCount is 1 on first delivery and goes up on each redelivery.
	DeliveryCount int
	// Consumer is the consumer that delivered it.
	Consumer string

	c *Client
}

func newMessage(c *Client, consumer string, w *proto.WireRecord) *Message {
	return &Message{Record: recordFromWire(w), DeliveryCount: int(w.DeliveryCount), Consumer: consumer, c: c}
}

// Ack acknowledges the message without waiting (fire-and-forget). Acks
// made close together share one frame; queued acks are always sent before
// any later request on the client. If the connection is down the ack is
// dropped and the record will be redelivered. A rejection by the server
// goes to the [WithErrorHandler] callback. Use [Message.AckSync] to wait
// for confirmation.
func (m *Message) Ack() { m.c.ackNowait(m.Consumer, m.Offset) }

// AckSync acknowledges the message and waits for the server to confirm.
func (m *Message) AckSync(ctx context.Context) error { return m.c.Ack(ctx, m.Consumer, m.Offset) }

// Nack asks for redelivery after delay (0 = the consumer's backoff).
func (m *Message) Nack(ctx context.Context, delay time.Duration) error {
	return m.c.Nack(ctx, m.Consumer, m.Offset, delay)
}

// Term never redelivers the message: it is dead-lettered now (to the
// consumer's DLQStream, if set), with reason in exspeed-dlq-reason.
func (m *Message) Term(ctx context.Context, reason string) error {
	return m.c.Term(ctx, m.Consumer, m.Offset, reason)
}

// InProgress resets the message's ack deadline (still working on it).
func (m *Message) InProgress(ctx context.Context) error {
	return m.c.InProgress(ctx, m.Consumer, m.Offset)
}

// signal is a broadcast: closing ch wakes every waiter; a new channel
// replaces it.
type signal struct{ ch chan struct{} }

func newSignal() signal { return signal{ch: make(chan struct{})} }

// fire wakes every waiter. Call with the owner's mutex held.
func (s *signal) fire() {
	close(s.ch)
	s.ch = make(chan struct{})
}

// Subscription is a push subscription to a consumer. Take messages with
// [Subscription.Next] or from the [Subscription.Messages] channel. It ends
// when the server ends it (see [SubscriptionEndedError]), on
// [Subscription.Unsubscribe], or when the client closes.
//
// Credit flow: the server pushes at most Window records ahead of the
// application. Each message taken returns one credit (sent in batches of
// half the window), so a slow consumer slows delivery instead of buffering
// without bound.
type Subscription struct {
	c        *Client
	consumer string
	window   int

	mu       sync.Mutex
	cn       *conn
	subID    uint32
	buf      []proto.WireRecord
	head     int
	consumed int
	wake     signal
	ended    *SubscriptionEndedError

	stopOnce sync.Once
	stopped  chan struct{} // closed on Unsubscribe / Close
	pumpOnce sync.Once
	msgs     chan *Message
}

func newSubscription(c *Client, consumer string, window int) *Subscription {
	return &Subscription{c: c, consumer: consumer, window: window, wake: newSignal(), stopped: make(chan struct{})}
}

// Consumer is the consumer this subscription takes records from.
func (s *Subscription) Consumer() string { return s.consumer }

// Window is the credit window.
func (s *Subscription) Window() int { return s.window }

// ID is the server-assigned id of the current subscription (it changes
// after a reconnect).
func (s *Subscription) ID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subID
}

// Buffered is the number of records received but not yet taken.
func (s *Subscription) Buffered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf) - s.head
}

// Closed reports whether the subscription has ended.
func (s *Subscription) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended != nil
}

// EndReason says why the subscription ended, or nil while it is active.
func (s *Subscription) EndReason() *SubscriptionEndedError {
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
// [*SubscriptionEndedError] (errors.Is(err, [ErrSubscriptionEnded])). When
// ctx is done first it returns ctx.Err(), and the subscription stays
// usable.
func (s *Subscription) Next(ctx context.Context) (*Message, error) {
	for {
		s.mu.Lock()
		if s.head < len(s.buf) {
			m, credit := s.takeLocked()
			s.mu.Unlock()
			credit()
			return m, nil
		}
		if s.ended != nil {
			e := *s.ended
			s.mu.Unlock()
			return nil, &e
		}
		wake := s.wake.ch
		s.mu.Unlock()
		s.c.flushAcks() // about to wait: send the acks of what was handled
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Messages returns a channel that delivers the subscription's messages and
// is closed when the subscription ends (then see [Subscription.EndReason]).
// The first call starts a goroutine that feeds the channel; it exits when
// the subscription ends. Use either Messages or Next, not both.
func (s *Subscription) Messages() <-chan *Message {
	s.pumpOnce.Do(func() {
		s.msgs = make(chan *Message)
		go s.pump()
	})
	return s.msgs
}

func (s *Subscription) pump() {
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

// Unsubscribe stops delivery. Records delivered to this subscription and
// not yet acked (including buffered ones never taken) are redelivered by
// the server.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
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

// takeLocked pops the next message. The returned func returns credit to
// the server when due; call it after releasing s.mu.
func (s *Subscription) takeLocked() (*Message, func()) {
	r := &s.buf[s.head]
	s.head++
	m := newMessage(s.c, s.consumer, r)
	s.buf[s.head-1] = proto.WireRecord{}
	if s.head == len(s.buf) {
		s.buf, s.head = s.buf[:0], 0
	} else if s.head > 1024 && s.head*2 > len(s.buf) {
		n := copy(s.buf, s.buf[s.head:])
		s.buf, s.head = s.buf[:n], 0
	}
	if s.cn != nil {
		s.consumed++
		if half := max(1, s.window/2); s.consumed >= half {
			cn, credit := s.cn, proto.Credit{SubID: s.subID, Credits: uint32(s.consumed)}
			s.consumed = 0
			return m, func() { _ = cn.send(credit) }
		}
	}
	return m, func() {}
}

// ---- sink (called on the connection's read goroutine) ----

func (s *Subscription) onSubscribed(cn *conn, id uint32) {
	s.mu.Lock()
	if s.ended != nil {
		// Unsubscribed while a (re-)subscribe was in flight.
		s.mu.Unlock()
		cn.removeSub(id)
		_ = cn.send(proto.Unsubscribe{SubID: id})
		return
	}
	s.cn, s.subID, s.consumed = cn, id, 0
	s.mu.Unlock()
}

func (s *Subscription) onDeliver(records []proto.WireRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended != nil {
		return
	}
	s.buf = append(s.buf, records...)
	s.wake.fire()
}

func (s *Subscription) onCoreMsg(*proto.CoreMsg) {}

// onEnded is a server-side end: buffered records are still yielded.
func (s *Subscription) onEnded(code uint16, message string) { s.end(int(code), message, true) }

// ---- client hooks ----

// suspend: the connection was lost; a re-subscribe follows. Buffered
// records are dropped (the server redelivers them).
func (s *Subscription) suspend() {
	s.mu.Lock()
	s.cn = nil
	s.buf, s.head, s.consumed = nil, 0, 0
	s.mu.Unlock()
}

func (s *Subscription) end(code int, message string, keepBuffered bool) {
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
	s.c.forgetSub(s)
}
