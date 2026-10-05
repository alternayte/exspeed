package exspeed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

type clientState int

const (
	stateConnected clientState = iota
	stateReconnecting
	stateClosed
)

// Client is a connection to an Exspeed server (client protocol v2).
//
// One client is one TCP or TLS connection. Requests are multiplexed by
// correlation id, so a slow pull or long-poll read never blocks other
// calls. A Client is safe for concurrent use; share one across your
// application.
//
// The connection is re-established automatically when it drops (see
// [WithReconnect]): pending requests fail with a [*ConnectionError], and
// subscriptions (consumer and core) are restored.
type Client struct {
	cfg    *clientConfig
	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc
	events *eventQueue

	mu        sync.Mutex
	conn      *conn
	state     clientState
	subs      map[*Subscription]struct{}
	coreSubs  map[*CoreSubscription]struct{}
	ephemeral []ConsumerSpec // re-created after a reconnect, in creation order
	inbox     *inbox
	replies   map[string]chan replyResult
	nextReply uint64

	ackSendMu   sync.Mutex // held while queued acks are taken and written
	ackMu       sync.Mutex
	pendingAcks map[string][]uint64
	ackOrder    []string
	ackCount    int
	ackTimer    *time.Timer
	ackArmed    bool

	wg sync.WaitGroup // reconnect and inbox goroutines
}

// Connect opens a connection to the server at addr ("host:port"; the port
// defaults to 5933, and "" means 127.0.0.1:5933) and authenticates. The
// first connection attempt is not retried. ctx bounds connecting only.
func Connect(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	if addr != "" {
		cfg.addr = addr
	}
	for _, o := range opts {
		o(cfg)
	}
	c := &Client{
		cfg:         cfg,
		subs:        make(map[*Subscription]struct{}),
		coreSubs:    make(map[*CoreSubscription]struct{}),
		replies:     make(map[string]chan replyResult),
		pendingAcks: make(map[string][]uint64),
		events:      newEventQueue(cfg.onDisconnect != nil || cfg.onReconnect != nil || cfg.onClose != nil || cfg.onError != nil),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	cn, err := c.openLeader(ctx, "")
	if err != nil {
		c.cancel()
		c.events.finish()
		return nil, err
	}
	c.mu.Lock()
	c.conn = cn
	c.mu.Unlock()
	if cn.isClosed() {
		c.onConnClosed(cn, &ConnectionError{Msg: "connection lost"})
	}
	return c, nil
}

// ServerInfo is the handshake info of the current connection.
func (c *Client) ServerInfo() ServerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.info
}

// Connected reports whether a connection is up (false while reconnecting
// and after Close).
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == stateConnected && !c.conn.isClosed()
}

// Close closes the connection. Queued acks are sent first. Pending requests
// fail with a [*ConnectionError] wrapping [ErrClosed], subscriptions end
// (code 0), and the server deletes this connection's ephemeral consumers.
// Close waits for the client's goroutines to exit. It is safe to call more
// than once.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.state == stateClosed {
		cn := c.conn
		c.mu.Unlock()
		cn.close()
		c.wg.Wait()
		return nil
	}
	c.mu.Unlock()
	c.flushAcks() // acks made just before Close still go out

	c.mu.Lock()
	if c.state == stateClosed {
		c.mu.Unlock()
		return nil
	}
	c.state = stateClosed
	cn := c.conn
	subs, coreSubs := c.takeSubsLocked()
	waiters := c.dropInboxLocked()
	c.mu.Unlock()

	c.cancel()
	closed := &ConnectionError{Msg: "client closed", Err: ErrClosed}
	for _, s := range subs {
		s.end(0, "client closed", false)
	}
	for _, s := range coreSubs {
		s.end(0, "client closed", false)
	}
	failReplies(waiters, closed)
	cn.close()
	c.wg.Wait()
	c.ackMu.Lock()
	if c.ackTimer != nil {
		c.ackTimer.Stop()
	}
	c.ackMu.Unlock()
	if f := c.cfg.onClose; f != nil {
		c.events.emit(func() { f(nil) })
	}
	c.events.finish()
	return nil
}

func (c *Client) takeSubsLocked() ([]*Subscription, []*CoreSubscription) {
	subs := make([]*Subscription, 0, len(c.subs))
	for s := range c.subs {
		subs = append(subs, s)
	}
	core := make([]*CoreSubscription, 0, len(c.coreSubs))
	for s := range c.coreSubs {
		core = append(core, s)
	}
	c.subs = make(map[*Subscription]struct{})
	c.coreSubs = make(map[*CoreSubscription]struct{})
	return subs, core
}

// ---- plumbing ---------------------------------------------------------------

// current is the connection to send on, or an error while reconnecting or
// after Close.
func (c *Client) current() (*conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case stateClosed:
		return nil, &ConnectionError{Msg: "client is closed", Err: ErrClosed}
	case stateReconnecting:
		return nil, &ConnectionError{Msg: "not connected (reconnecting)"}
	}
	return c.conn, nil
}

func (c *Client) do(ctx context.Context, m proto.Message, o reqOpts) (proto.Message, error) {
	cn, err := c.current()
	if err != nil {
		return nil, err
	}
	c.flushAcks() // keeps wire order equal to call order
	return cn.request(ctx, m, o)
}

func call[T proto.Message](ctx context.Context, c *Client, m proto.Message, o reqOpts) (T, error) {
	var zero T
	resp, err := c.do(ctx, m, o)
	if err != nil {
		return zero, err
	}
	t, ok := resp.(T)
	if !ok {
		return zero, &ProtocolError{Msg: fmt.Sprintf("unexpected reply to %s: %s", opName(m), opName(resp))}
	}
	return t, nil
}

func decodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return &ProtocolError{Msg: "bad JSON reply: " + err.Error()}
	}
	return nil
}

func (c *Client) callJSON(ctx context.Context, m proto.Message, v any) error {
	r, err := call[proto.JSON](ctx, c, m, reqOpts{})
	if err != nil {
		return err
	}
	return decodeJSON(r.Data, v)
}

func (c *Client) callOk(ctx context.Context, m proto.Message) error {
	_, err := call[proto.Ok](ctx, c, m, reqOpts{})
	return err
}

func (c *Client) asyncError(err error) {
	if f := c.cfg.onError; f != nil {
		c.events.emit(func() { f(err) })
	}
}

func (c *Client) handlers() connHandlers {
	return connHandlers{onClose: c.onConnClosed, onAsyncError: c.asyncError}
}

// ---- basics -----------------------------------------------------------------

// Ping makes a round trip to the server and returns its latency.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	if _, err := call[proto.Pong](ctx, c, proto.Ping{}, reqOpts{}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// Metadata returns the node id, leadership and server version.
func (c *Client) Metadata(ctx context.Context) (*Metadata, error) {
	var raw struct {
		NodeID        string  `json:"node_id"`
		IsLeader      bool    `json:"is_leader"`
		Leader        *string `json:"leader"`
		ServerVersion string  `json:"server_version"`
	}
	if err := c.callJSON(ctx, proto.Metadata{}, &raw); err != nil {
		return nil, err
	}
	m := &Metadata{NodeID: raw.NodeID, IsLeader: raw.IsLeader, ServerVersion: raw.ServerVersion}
	if raw.Leader != nil {
		m.Leader = *raw.Leader
	}
	return m, nil
}

// ---- streams ----------------------------------------------------------------

// CreateStream creates a stream. It is idempotent when the stream exists
// with the same settings, and fails with 409 when they differ.
func (c *Client) CreateStream(ctx context.Context, spec StreamSpec) error {
	w, err := spec.toWire()
	if err != nil {
		return err
	}
	return c.callOk(ctx, proto.CreateStream{Spec: w})
}

// UpdateStream replaces a stream's settings: omitted fields reset to the
// server defaults, so pass every field you want to keep.
func (c *Client) UpdateStream(ctx context.Context, spec StreamSpec) error {
	w, err := spec.toWire()
	if err != nil {
		return err
	}
	return c.callOk(ctx, proto.UpdateStream{Spec: w})
}

// DeleteStream deletes a stream. It fails with 409 while consumers exist
// (detail.consumers names them).
func (c *Client) DeleteStream(ctx context.Context, name string) error {
	return c.callOk(ctx, proto.DeleteStream{Name: name})
}

// StreamInfo describes a stream.
func (c *Client) StreamInfo(ctx context.Context, name string) (*StreamInfo, error) {
	r, err := call[proto.JSON](ctx, c, proto.StreamInfo{Name: name}, reqOpts{})
	if err != nil {
		return nil, err
	}
	info := &StreamInfo{}
	if err := decodeJSON(r.Data, info); err != nil {
		return nil, err
	}
	info.Raw = append(json.RawMessage(nil), r.Data...)
	return info, nil
}

// ListStreams lists the streams this credential can see.
func (c *Client) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	var raws []json.RawMessage
	if err := c.callJSON(ctx, proto.ListStreams{}, &raws); err != nil {
		return nil, err
	}
	out := make([]StreamInfo, len(raws))
	for i, raw := range raws {
		if err := decodeJSON(raw, &out[i]); err != nil {
			return nil, err
		}
		out[i].Raw = raw
	}
	return out, nil
}

// ---- publishing ---------------------------------------------------------------

// Publish appends one record and waits for its offset.
func (c *Client) Publish(ctx context.Context, stream string, rec PublishRecord) (PublishResult, error) {
	w, err := rec.toWire()
	if err != nil {
		return PublishResult{}, err
	}
	r, err := call[proto.PublishOk](ctx, c, proto.Publish{Stream: stream, Record: w}, reqOpts{})
	if err != nil {
		return PublishResult{}, err
	}
	return PublishResult{Offset: r.Offset, Duplicate: r.Duplicate}, nil
}

// PublishBatch appends several records in one request and returns one
// result per record, in order. A batch is applied as a whole or fails as a
// whole.
func (c *Client) PublishBatch(ctx context.Context, stream string, recs []PublishRecord) ([]PublishResult, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	ws := make([]proto.PublishRecord, len(recs))
	for i := range recs {
		w, err := recs[i].toWire()
		if err != nil {
			return nil, err
		}
		ws[i] = w
	}
	r, err := call[proto.PublishBatchOk](ctx, c, proto.PublishBatch{Stream: stream, Records: ws}, reqOpts{})
	if err != nil {
		return nil, err
	}
	if len(r.Results) != len(recs) {
		return nil, &ProtocolError{Msg: fmt.Sprintf("PublishBatchOk has %d results for %d records", len(r.Results), len(recs))}
	}
	out := make([]PublishResult, len(r.Results))
	for i, x := range r.Results {
		out[i] = PublishResult{Offset: x.Offset, Duplicate: x.Duplicate}
	}
	return out, nil
}

// requestAsync sends m on the current connection and calls cb with the
// reply (see conn.requestAsync).
func (c *Client) requestAsync(m proto.Message, cb func(proto.Message, error)) error {
	cn, err := c.current()
	if err != nil {
		return err
	}
	c.flushAcks()
	return cn.requestAsync(m, cb)
}

// ---- stateless reads ----------------------------------------------------------

// Read reads records without a consumer. With Wait, it long-polls when
// caught up. Continue from NextOffset.
func (c *Client) Read(ctx context.Context, stream string, opts ReadOptions) (*ReadResult, error) {
	maxRecords := opts.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 100
	}
	if opts.Wait < 0 || opts.MaxBytes < 0 {
		return nil, invalidf("Wait and MaxBytes must not be negative")
	}
	waitMs := ceilUnits(opts.Wait, time.Millisecond)
	req := proto.Read{
		Stream:     stream,
		From:       opts.From,
		MaxRecords: clampU32(maxRecords),
		MaxBytes:   clampU32(opts.MaxBytes),
		WaitMs:     uint32(min(waitMs, 0xffffffff)),
		Filter:     opts.Filter,
	}
	r, err := call[proto.ReadResult](ctx, c, req, reqOpts{extra: opts.Wait})
	if err != nil {
		return nil, err
	}
	out := &ReadResult{NextOffset: r.NextOffset, HighWatermark: r.HighWatermark, Records: make([]Record, len(r.Records))}
	for i := range r.Records {
		out.Records[i] = recordFromWire(&r.Records[i])
	}
	return out, nil
}

func clampU32(v int) uint32 {
	if v <= 0 {
		return 0
	}
	if int64(v) > 0xffffffff {
		return 0xffffffff
	}
	return uint32(v)
}

// ---- queries --------------------------------------------------------------------

// Query runs a bounded ExQL query. With auth on, it needs a global-admin
// credential.
func (c *Client) Query(ctx context.Context, sql string) (*QueryResult, error) {
	r := &QueryResult{}
	if err := c.callJSON(ctx, proto.Query{SQL: sql}, r); err != nil {
		return nil, err
	}
	return r, nil
}

// ---- consumers ------------------------------------------------------------------

// CreateConsumer creates a consumer. It is idempotent for an identical
// spec, and fails with 409 when a consumer of that name exists with a
// different spec.
func (c *Client) CreateConsumer(ctx context.Context, spec ConsumerSpec) (*ConsumerInfo, error) {
	w, err := spec.toWire()
	if err != nil {
		return nil, err
	}
	b, err := marshalJSON(w)
	if err != nil {
		return nil, err
	}
	r, err := call[proto.JSON](ctx, c, proto.CreateConsumer{Spec: b}, reqOpts{})
	if err != nil {
		return nil, err
	}
	info, err := parseConsumerInfo(r.Data)
	if err != nil {
		return nil, err
	}
	if spec.Ephemeral {
		c.mu.Lock()
		c.ephemeral = append(removeSpec(c.ephemeral, spec.Name), spec)
		c.mu.Unlock()
	}
	return info, nil
}

func removeSpec(specs []ConsumerSpec, name string) []ConsumerSpec {
	out := specs[:0]
	for _, s := range specs {
		if s.Name != name {
			out = append(out, s)
		}
	}
	return out
}

func parseConsumerInfo(data []byte) (*ConsumerInfo, error) {
	info := &ConsumerInfo{}
	if err := decodeJSON(data, info); err != nil {
		return nil, err
	}
	info.Raw = append(json.RawMessage(nil), data...)
	return info, nil
}

// DeleteConsumer deletes a consumer.
func (c *Client) DeleteConsumer(ctx context.Context, name string) error {
	if err := c.callOk(ctx, proto.DeleteConsumer{Name: name}); err != nil {
		return err
	}
	c.mu.Lock()
	c.ephemeral = removeSpec(c.ephemeral, name)
	c.mu.Unlock()
	return nil
}

// ConsumerInfo returns a consumer's spec, position and counters.
func (c *Client) ConsumerInfo(ctx context.Context, name string) (*ConsumerInfo, error) {
	r, err := call[proto.JSON](ctx, c, proto.ConsumerInfo{Name: name}, reqOpts{})
	if err != nil {
		return nil, err
	}
	return parseConsumerInfo(r.Data)
}

// ListConsumers lists the consumers this credential can see; stream ""
// lists those of every stream.
func (c *Client) ListConsumers(ctx context.Context, stream string) ([]ConsumerInfo, error) {
	req := proto.ListConsumers{}
	if stream != "" {
		req.Stream = &stream
	}
	var raws []json.RawMessage
	if err := c.callJSON(ctx, req, &raws); err != nil {
		return nil, err
	}
	out := make([]ConsumerInfo, len(raws))
	for i, raw := range raws {
		info, err := parseConsumerInfo(raw)
		if err != nil {
			return nil, err
		}
		out[i] = *info
	}
	return out, nil
}

// Seek moves a consumer's cursor.
func (c *Client) Seek(ctx context.Context, consumer string, to SeekTarget) error {
	return c.callOk(ctx, proto.SeekConsumer{Consumer: consumer, Kind: to.kind, Value: to.value})
}

// Subscribe starts push delivery from a consumer. Any number of
// subscriptions (on any connection, in any process) can share one
// consumer; each record goes to one of them.
func (c *Client) Subscribe(ctx context.Context, consumer string, opts SubscribeOptions) (*Subscription, error) {
	window := opts.Window
	if window <= 0 {
		window = 256
	}
	if int64(window) > 0xffffffff {
		window = 0xffffffff
	}
	s := newSubscription(c, consumer, window)
	// The connection binds s to its id as soon as SubscribeOk arrives,
	// before any Deliver behind it is routed.
	if _, err := call[proto.SubscribeOk](ctx, c, proto.Subscribe{Consumer: consumer, Credits: uint32(window)}, reqOpts{sink: s}); err != nil {
		s.end(0, "subscribe failed", false)
		return nil, err
	}
	c.mu.Lock()
	if c.state == stateClosed {
		c.mu.Unlock()
		s.end(0, "client closed", false)
		return s, nil
	}
	c.subs[s] = struct{}{}
	c.mu.Unlock()
	return s, nil
}

func (c *Client) forgetSub(s *Subscription) {
	c.mu.Lock()
	delete(c.subs, s)
	c.mu.Unlock()
}

// Pull fetches up to MaxMessages from a consumer, waiting up to Expires for
// at least one. It returns an empty slice on timeout.
func (c *Client) Pull(ctx context.Context, consumer string, opts PullOptions) ([]*Message, error) {
	maxMessages := opts.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 100
	}
	expires := opts.Expires
	if expires <= 0 {
		expires = 5 * time.Second
	}
	if opts.NoWait {
		expires = 0
	}
	expiresMs := ceilUnits(expires, time.Millisecond)
	req := proto.Pull{
		Consumer:    consumer,
		MaxMessages: clampU32(maxMessages),
		MaxBytes:    clampU32(opts.MaxBytes),
		ExpiresMs:   uint32(min(expiresMs, 0xffffffff)),
	}
	r, err := call[proto.Messages](ctx, c, req, reqOpts{extra: expires})
	if err != nil {
		return nil, err
	}
	out := make([]*Message, len(r.Records))
	for i := range r.Records {
		out[i] = newMessage(c, consumer, &r.Records[i])
	}
	return out, nil
}

// Ack acknowledges records and waits for the server to confirm. See also
// [Message.Ack], which doesn't wait.
func (c *Client) Ack(ctx context.Context, consumer string, offsets ...uint64) error {
	return c.callOk(ctx, proto.Ack{Consumer: consumer, Offsets: offsets})
}

// Nack asks for redelivery after delay (0 = the consumer's backoff).
func (c *Client) Nack(ctx context.Context, consumer string, offset uint64, delay time.Duration) error {
	ms := ceilUnits(delay, time.Millisecond)
	return c.callOk(ctx, proto.Nack{Consumer: consumer, Offset: offset, DelayMs: uint32(min(ms, 0xffffffff))})
}

// Term dead-letters a record now (to the consumer's DLQStream, if set).
func (c *Client) Term(ctx context.Context, consumer string, offset uint64, reason string) error {
	return c.callOk(ctx, proto.Term{Consumer: consumer, Offset: offset, Reason: reason})
}

// InProgress resets the ack deadlines of records still being worked on.
func (c *Client) InProgress(ctx context.Context, consumer string, offsets ...uint64) error {
	return c.callOk(ctx, proto.InProgress{Consumer: consumer, Offsets: offsets})
}

// ackNowait queues a fire-and-forget ack. Queued acks go out as one Ack
// frame per consumer: before any later request (so wire order matches call
// order), when a subscription runs out of buffered messages, when many are
// queued, or after a short linger.
func (c *Client) ackNowait(consumer string, offset uint64) {
	c.mu.Lock()
	connected := c.state == stateConnected
	c.mu.Unlock()
	if !connected {
		return // redelivered after the reconnect
	}
	c.ackMu.Lock()
	if _, ok := c.pendingAcks[consumer]; !ok {
		c.ackOrder = append(c.ackOrder, consumer)
	}
	c.pendingAcks[consumer] = append(c.pendingAcks[consumer], offset)
	c.ackCount++
	full := c.ackCount >= c.cfg.ackFlushAt
	if !full && !c.ackArmed {
		c.ackArmed = true
		if c.ackTimer == nil {
			c.ackTimer = time.AfterFunc(c.cfg.ackLinger, c.flushAcks)
		} else {
			c.ackTimer.Reset(c.cfg.ackLinger)
		}
	}
	c.ackMu.Unlock()
	if full {
		c.flushAcks()
	}
}

// flushAcks sends every queued ack.
func (c *Client) flushAcks() {
	c.ackSendMu.Lock()
	defer c.ackSendMu.Unlock()
	c.ackMu.Lock()
	if c.ackCount == 0 {
		c.ackMu.Unlock()
		return
	}
	acks, order := c.pendingAcks, c.ackOrder
	c.pendingAcks, c.ackOrder, c.ackCount = make(map[string][]uint64), nil, 0
	if c.ackArmed {
		c.ackArmed = false
		c.ackTimer.Stop()
	}
	c.ackMu.Unlock()
	c.mu.Lock()
	cn := c.conn
	ok := c.state == stateConnected
	c.mu.Unlock()
	if !ok {
		return
	}
	for _, consumer := range order {
		_ = cn.send(proto.Ack{Consumer: consumer, Offsets: acks[consumer]})
	}
}

func (c *Client) clearAcks() {
	c.ackMu.Lock()
	c.pendingAcks, c.ackOrder, c.ackCount = make(map[string][]uint64), nil, 0
	c.ackMu.Unlock()
}

// ---- key-value ----------------------------------------------------------------

// KV returns a handle to the key-value bucket named bucket (create the
// bucket with [KV.Create]).
func (c *Client) KV(bucket string) *KV { return &KV{c: c, bucket: bucket} }

// ---- connection management ----------------------------------------------------

// onConnClosed is called when a connection closes for a reason other than
// conn.close().
func (c *Client) onConnClosed(cn *conn, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stateConnected || c.conn != cn {
		return
	}
	c.wg.Add(1)
	go c.handleConnLost(cn, err)
}

func (c *Client) handleConnLost(cn *conn, err error) {
	defer c.wg.Done()
	c.mu.Lock()
	if c.state != stateConnected || c.conn != cn {
		c.mu.Unlock()
		return
	}
	// Responses to the inbox can't arrive any more; a request after the
	// reconnect subscribes a new inbox.
	waiters := c.dropInboxLocked()
	lost := &ConnectionError{Msg: "connection lost", Err: err}
	if c.cfg.reconnect == nil {
		c.state = stateClosed
		subs, coreSubs := c.takeSubsLocked()
		c.mu.Unlock()
		c.cancel()
		failReplies(waiters, lost)
		for _, s := range subs {
			s.end(CodeUnavailable, "connection closed", false)
		}
		for _, s := range coreSubs {
			s.end(CodeUnavailable, "connection closed", false)
		}
		if f := c.cfg.onClose; f != nil {
			c.events.emit(func() { f(err) })
		}
		c.events.finish()
		return
	}
	c.state = stateReconnecting
	for s := range c.subs {
		s.suspend()
	}
	for s := range c.coreSubs {
		s.suspend()
	}
	c.mu.Unlock()
	c.clearAcks() // those records will be redelivered
	failReplies(waiters, lost)
	if f := c.cfg.onDisconnect; f != nil {
		c.events.emit(func() { f(err) })
	}
	c.reconnectLoop(cn.info.Leader)
}

func (c *Client) reconnectLoop(hint string) {
	p := c.cfg.reconnect
	var lastErr error = &ConnectionError{Msg: "connection lost"}
	for attempt := 1; p.MaxAttempts == 0 || attempt <= p.MaxAttempts; attempt++ {
		delay := p.InitialDelay
		for i := 1; i < attempt && delay < p.MaxDelay; i++ {
			delay *= 2
		}
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
		delay = time.Duration(float64(delay) * (0.75 + rand.Float64()*0.5))
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-c.ctx.Done():
			t.Stop()
			return
		}
		cn, err := c.openLeader(c.ctx, hint)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			lastErr = err
			// A rejected credential won't get better by retrying.
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) {
				break
			}
			continue
		}
		c.mu.Lock()
		if c.state != stateReconnecting {
			c.mu.Unlock()
			cn.close()
			return
		}
		c.conn = cn
		c.state = stateConnected
		c.mu.Unlock()
		if cn.isClosed() {
			c.onConnClosed(cn, &ConnectionError{Msg: "connection lost"})
			return
		}
		c.restore(cn)
		c.mu.Lock()
		ok := c.conn == cn && c.state == stateConnected
		c.mu.Unlock()
		if ok {
			if f := c.cfg.onReconnect; f != nil {
				info := cn.info
				c.events.emit(func() { f(info) })
			}
		}
		return
	}
	c.mu.Lock()
	if c.state != stateReconnecting {
		c.mu.Unlock()
		return
	}
	c.state = stateClosed
	subs, coreSubs := c.takeSubsLocked()
	c.mu.Unlock()
	c.cancel()
	msg := "connection lost: " + lastErr.Error()
	for _, s := range subs {
		s.end(CodeUnavailable, msg, false)
	}
	for _, s := range coreSubs {
		s.end(CodeUnavailable, msg, false)
	}
	if f := c.cfg.onClose; f != nil {
		c.events.emit(func() { f(lastErr) })
	}
	c.events.finish()
}

// restore re-creates ephemeral consumers, then re-subscribes every live
// subscription (consumer and core).
func (c *Client) restore(cn *conn) {
	c.mu.Lock()
	specs := append([]ConsumerSpec(nil), c.ephemeral...)
	subs := make([]*Subscription, 0, len(c.subs))
	for s := range c.subs {
		subs = append(subs, s)
	}
	core := make([]*CoreSubscription, 0, len(c.coreSubs))
	for s := range c.coreSubs {
		core = append(core, s)
	}
	c.mu.Unlock()
	var connErr *ConnectionError
	for _, spec := range specs {
		w, err := spec.toWire()
		if err != nil {
			continue
		}
		b, err := marshalJSON(w)
		if err != nil {
			continue
		}
		if _, err := cn.request(c.ctx, proto.CreateConsumer{Spec: b}, reqOpts{}); errors.As(err, &connErr) || c.ctx.Err() != nil {
			return // lost again; the next loop retries
		}
	}
	var wg sync.WaitGroup
	for _, s := range subs {
		wg.Add(1)
		go func(s *Subscription) {
			defer wg.Done()
			_, err := cn.request(c.ctx, proto.Subscribe{Consumer: s.consumer, Credits: uint32(s.window)}, reqOpts{sink: s})
			if err == nil || errors.As(err, &connErr) || c.ctx.Err() != nil {
				return // on a connection error it stays suspended for the next attempt
			}
			code, msg := errorCode(err)
			s.end(code, msg, false)
		}(s)
	}
	for _, s := range core {
		wg.Add(1)
		go func(s *CoreSubscription) {
			defer wg.Done()
			_, err := cn.request(c.ctx, proto.CoreSubscribe{Subject: s.subject, Queue: s.queuePtr()}, reqOpts{sink: s})
			if err == nil || errors.As(err, &connErr) || c.ctx.Err() != nil {
				return
			}
			code, msg := errorCode(err)
			s.end(code, msg, true)
		}(s)
	}
	wg.Wait()
}

func errorCode(err error) (int, string) {
	var se *ServerError
	if errors.As(err, &se) {
		return se.Code, se.Message
	}
	return CodeInternal, err.Error()
}

// normalizeAddr adds the default port when addr has none.
func normalizeAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	host := strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
	return net.JoinHostPort(host, strconv.Itoa(DefaultPort))
}

// openLeader opens a connection to the cluster leader. Without seed
// servers this is a plain connect to the configured address, except that a
// node naming another node as leader in its handshake is followed. With
// seeds, each candidate (the last known leader first) is asked whether it
// leads; leader hints are followed, and a follower is accepted only when no
// node claims to lead.
func (c *Client) openLeader(ctx context.Context, hint string) (*conn, error) {
	var queue []string
	if hint != "" {
		queue = append(queue, hint)
	}
	if len(c.cfg.servers) > 0 {
		queue = append(queue, c.cfg.servers...)
	} else {
		queue = append(queue, c.cfg.addr)
	}
	tried := map[string]bool{}
	var fallback *conn
	var lastErr error
	cc := c.cfg.connConfig()
	for len(queue) > 0 {
		addr := normalizeAddr(queue[0])
		queue = queue[1:]
		if tried[addr] {
			continue
		}
		tried[addr] = true
		cn, err := openConn(ctx, addr, cc, c.handlers())
		if err != nil {
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) || ctx.Err() != nil {
				if fallback != nil {
					fallback.close()
				}
				return nil, err
			}
			lastErr = err
			continue
		}
		isLeader := cn.info.Leader == ""
		leader := cn.info.Leader
		if len(c.cfg.servers) > 0 {
			if r, err := cn.request(ctx, proto.Metadata{}, reqOpts{}); err == nil {
				if j, ok := r.(proto.JSON); ok {
					var m struct {
						IsLeader bool    `json:"is_leader"`
						Leader   *string `json:"leader"`
					}
					if json.Unmarshal(j.Data, &m) == nil {
						isLeader = m.IsLeader
						leader = ""
						if m.Leader != nil {
							leader = *m.Leader
						}
					}
				}
			}
		}
		if isLeader {
			if fallback != nil {
				fallback.close()
			}
			return cn, nil
		}
		if leader != "" && !tried[normalizeAddr(leader)] {
			queue = append([]string{leader}, queue...)
		}
		if fallback == nil {
			fallback = cn
		} else {
			cn.close()
		}
	}
	if fallback != nil {
		return fallback, nil
	}
	if lastErr == nil {
		lastErr = &ConnectionError{Msg: "no server reachable"}
	}
	return nil, lastErr
}

// ---- events -------------------------------------------------------------------

// eventQueue runs user callbacks, in order, on one goroutine of their own,
// so a slow callback never stalls the connection. The goroutine exits
// after finish, once the queue is drained.
type eventQueue struct {
	enabled bool
	mu      sync.Mutex
	q       []func()
	wake    chan struct{}
	done    bool
}

func newEventQueue(enabled bool) *eventQueue {
	e := &eventQueue{enabled: enabled, wake: make(chan struct{}, 1)}
	if enabled {
		go e.run()
	}
	return e
}

func (e *eventQueue) run() {
	for {
		e.mu.Lock()
		if len(e.q) == 0 {
			done := e.done
			e.mu.Unlock()
			if done {
				return
			}
			<-e.wake
			continue
		}
		f := e.q[0]
		e.q = e.q[1:]
		e.mu.Unlock()
		f()
	}
}

func (e *eventQueue) emit(f func()) {
	if !e.enabled {
		return
	}
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		return
	}
	e.q = append(e.q, f)
	e.mu.Unlock()
	e.signal()
}

func (e *eventQueue) finish() {
	e.mu.Lock()
	e.done = true
	e.mu.Unlock()
	e.signal()
}

func (e *eventQueue) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
