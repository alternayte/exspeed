package exspeed

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// DefaultPort is the server's default client port.
const DefaultPort = proto.DefaultPort

// ProtocolVersion is the wire protocol version this client speaks.
const ProtocolVersion = int(proto.Version)

// Header names behind the time and priority publish options.
const (
	TTLHeader       = "exspeed-ttl"
	DelayHeader     = "exspeed-delay"
	DeliverAtHeader = "exspeed-deliver-at"
	PriorityHeader  = "exspeed-priority"
)

// Header is one record or message header. Keys may repeat; order is kept.
type Header struct {
	Key   string
	Value string
}

// Headers builds a header list from key, value pairs:
//
//	exspeed.Headers("trace-id", "abc", "tenant", "acme")
//
// It panics when given an odd number of strings.
func Headers(pairs ...string) []Header {
	if len(pairs)%2 != 0 {
		panic("exspeed.Headers: odd number of arguments")
	}
	out := make([]Header, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, Header{Key: pairs[i], Value: pairs[i+1]})
	}
	return out
}

func toWireHeaders(h []Header) []proto.Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]proto.Header, len(h))
	for i, kv := range h {
		out[i] = proto.Header{Key: kv.Key, Value: kv.Value}
	}
	return out
}

func fromWireHeaders(h []proto.Header) []Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]Header, len(h))
	for i, kv := range h {
		out[i] = Header{Key: kv.Key, Value: kv.Value}
	}
	return out
}

func findHeader(h []Header, name string) (string, bool) {
	for _, kv := range h {
		if kv.Key == name {
			return kv.Value, true
		}
	}
	return "", false
}

// ServerInfo comes from the server's handshake reply.
type ServerInfo struct {
	ServerVersion string
	NodeID        string
	// Leader is the leader's client address when the connected node is not
	// the leader, else "".
	Leader string
}

// Metadata is the reply to [Client.Metadata].
type Metadata struct {
	NodeID        string `json:"node_id"`
	IsLeader      bool   `json:"is_leader"`
	Leader        string `json:"leader"`
	ServerVersion string `json:"server_version"`
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

// DiscardPolicy says what happens when a stream reaches MaxMsgs.
type DiscardPolicy string

// Discard policies. The empty value is the server default (old).
const (
	DiscardOld DiscardPolicy = "old"
	DiscardNew DiscardPolicy = "new"
)

// RetentionPolicy says when records leave a stream besides its limits.
type RetentionPolicy string

// Retention policies. The empty value is the server default (limits).
const (
	// RetentionLimits: records stay until a limit removes them.
	RetentionLimits RetentionPolicy = "limits"
	// RetentionWorkQueue: at most one consumer; a record is removed once acked.
	RetentionWorkQueue RetentionPolicy = "work_queue"
	// RetentionInterest: a record is removed once every consumer acked it.
	RetentionInterest RetentionPolicy = "interest"
)

// StreamSpec is a stream's settings. Zero values mean "server default".
type StreamSpec struct {
	Name string
	// MaxAge is retention by age (rounded up to whole seconds).
	MaxAge time.Duration
	// MaxBytes is retention by size.
	MaxBytes uint64
	// DedupWindow is how long msg ids are remembered (whole seconds).
	DedupWindow time.Duration
	// DedupMaxEntries bounds the dedup map.
	DedupMaxEntries uint64
	// Compaction keeps only the latest record per key.
	Compaction bool
	// MaxMsgs is the most records the stream holds; 0 = no limit. What
	// happens at the limit is Discard.
	MaxMsgs uint64
	// Discard at MaxMsgs: DiscardOld drops the oldest records, DiscardNew
	// rejects new ones (429).
	Discard DiscardPolicy
	// MaxMsgsPerSubject is the most records kept per subject; 0 = no limit.
	MaxMsgsPerSubject uint64
	// AllowMsgTTL accepts the per-record TTL publish option.
	AllowMsgTTL bool
	// MsgTTL is the default lifetime of every record (whole ms); 0 = none.
	MsgTTL time.Duration
	// AllowDelayed accepts the Delay / DeliverAt publish options.
	AllowDelayed bool
	// Retention policy; see [RetentionPolicy].
	Retention RetentionPolicy
	// CaptureSubjects are subject filters: core messages published to a
	// matching subject are also appended to this stream. No two streams may
	// capture overlapping subjects.
	CaptureSubjects []string
}

// wireLimits is StreamLimits (crates/exspeed-common/src/limits.rs) exactly
// as serde serializes it: snake_case keys in declaration order.
type wireLimits struct {
	MaxMsgs           uint64   `json:"max_msgs"`
	Discard           string   `json:"discard"`
	MaxMsgsPerSubject uint64   `json:"max_msgs_per_subject"`
	AllowMsgTTL       bool     `json:"allow_msg_ttl"`
	MsgTTLMs          uint64   `json:"msg_ttl_ms"`
	AllowDelayed      bool     `json:"allow_delayed"`
	Retention         string   `json:"retention"`
	CaptureSubjects   []string `json:"capture_subjects,omitempty"`
}

func ceilUnits(d, unit time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64((d + unit - 1) / unit)
}

func (s *StreamSpec) toWire() (proto.StreamSpec, error) {
	if s.Name == "" {
		return proto.StreamSpec{}, invalidf("stream name is required")
	}
	if s.MaxAge < 0 || s.DedupWindow < 0 || s.MsgTTL < 0 {
		return proto.StreamSpec{}, invalidf("stream durations must not be negative")
	}
	w := proto.StreamSpec{
		Name:            s.Name,
		MaxAgeSecs:      ceilUnits(s.MaxAge, time.Second),
		MaxBytes:        s.MaxBytes,
		DedupWindowSecs: ceilUnits(s.DedupWindow, time.Second),
		DedupMaxEntries: s.DedupMaxEntries,
		Compaction:      s.Compaction,
	}
	l := wireLimits{
		MaxMsgs:           s.MaxMsgs,
		Discard:           string(s.Discard),
		MaxMsgsPerSubject: s.MaxMsgsPerSubject,
		AllowMsgTTL:       s.AllowMsgTTL,
		MsgTTLMs:          ceilUnits(s.MsgTTL, time.Millisecond),
		AllowDelayed:      s.AllowDelayed,
		Retention:         string(s.Retention),
	}
	if len(s.CaptureSubjects) > 0 {
		l.CaptureSubjects = append([]string(nil), s.CaptureSubjects...)
	}
	if l.Discard == "" {
		l.Discard = string(DiscardOld)
	}
	if l.Retention == "" {
		l.Retention = string(RetentionLimits)
	}
	isDefault := l.MaxMsgs == 0 && l.Discard == "old" && l.MaxMsgsPerSubject == 0 && !l.AllowMsgTTL &&
		l.MsgTTLMs == 0 && !l.AllowDelayed && l.Retention == "limits" && len(l.CaptureSubjects) == 0
	if !isDefault {
		b, err := marshalJSON(l)
		if err != nil {
			return proto.StreamSpec{}, err
		}
		w.Limits = b
	}
	return w, nil
}

// StreamInfo describes a stream.
type StreamInfo struct {
	Name           string       `json:"name"`
	EarliestOffset uint64       `json:"earliest_offset"`
	NextOffset     uint64       `json:"next_offset"`
	Records        uint64       `json:"records"`
	Config         StreamConfig `json:"config"`
	Internal       bool         `json:"internal"`
	// Raw is the JSON the server sent.
	Raw json.RawMessage `json:"-"`
}

// StreamConfig is a stream's effective settings, as the server reports them.
type StreamConfig struct {
	MaxAgeSecs             uint64          `json:"max_age_secs"`
	MaxBytes               uint64          `json:"max_bytes"`
	DedupWindowSecs        uint64          `json:"dedup_window_secs"`
	DedupMaxEntries        uint64          `json:"dedup_max_entries"`
	Compaction             bool            `json:"compaction"`
	TombstoneRetentionSecs uint64          `json:"tombstone_retention_secs"`
	MaxMsgs                uint64          `json:"max_msgs"`
	Discard                DiscardPolicy   `json:"discard"`
	MaxMsgsPerSubject      uint64          `json:"max_msgs_per_subject"`
	AllowMsgTTL            bool            `json:"allow_msg_ttl"`
	MsgTTLMs               uint64          `json:"msg_ttl_ms"`
	AllowDelayed           bool            `json:"allow_delayed"`
	Retention              RetentionPolicy `json:"retention"`
	CaptureSubjects        []string        `json:"capture_subjects"`
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

// PublishRecord is a record to publish.
type PublishRecord struct {
	Subject string
	// Key is the compaction key; nil = no key.
	Key   []byte
	Value []byte
	// Headers go out in order, before the headers of the options below.
	Headers []Header
	// MsgID is an idempotency key: a retry with the same MsgID and body
	// returns the original offset with Duplicate set instead of writing
	// again. See [NewMsgID].
	MsgID string
	// TTL expires the record this long after the append (rounded up to whole
	// ms; header exspeed-ttl). The stream needs AllowMsgTTL.
	TTL time.Duration
	// Delay holds the record back from consumers for this long after the
	// append (header exspeed-delay). The stream needs AllowDelayed.
	Delay time.Duration
	// DeliverAt holds the record back from consumers until this time
	// (header exspeed-deliver-at, ms since the epoch). The stream needs
	// AllowDelayed.
	DeliverAt time.Time
	// Priority is 0 (default) to 9, higher first, for consumers with a
	// PriorityWindow (header exspeed-priority).
	Priority int
}

func (r *PublishRecord) toWire() (proto.PublishRecord, error) {
	if r.TTL < 0 || r.Delay < 0 {
		return proto.PublishRecord{}, invalidf("ttl and delay must not be negative")
	}
	if r.Priority < 0 || r.Priority > 9 {
		return proto.PublishRecord{}, invalidf("priority must be from 0 to 9, got %d", r.Priority)
	}
	headers := toWireHeaders(r.Headers)
	if r.TTL > 0 {
		headers = append(headers, proto.Header{Key: TTLHeader, Value: msString(r.TTL)})
	}
	if r.Delay > 0 {
		headers = append(headers, proto.Header{Key: DelayHeader, Value: msString(r.Delay)})
	}
	if !r.DeliverAt.IsZero() {
		ms := r.DeliverAt.UnixMilli()
		if ms < 0 {
			return proto.PublishRecord{}, invalidf("invalid DeliverAt %v", r.DeliverAt)
		}
		headers = append(headers, proto.Header{Key: DeliverAtHeader, Value: strconv.FormatInt(ms, 10)})
	}
	if r.Priority != 0 {
		headers = append(headers, proto.Header{Key: PriorityHeader, Value: strconv.Itoa(r.Priority)})
	}
	w := proto.PublishRecord{Subject: r.Subject, Key: r.Key, Value: r.Value, Headers: headers}
	if w.Value == nil {
		w.Value = []byte{}
	}
	if r.MsgID != "" {
		id := r.MsgID
		w.MsgID = &id
	}
	return w, nil
}

// msString renders a duration as whole milliseconds ("<n>ms"), rounded up.
func msString(d time.Duration) string {
	return strconv.FormatUint(ceilUnits(d, time.Millisecond), 10) + "ms"
}

// PublishResult is the outcome of one published record.
type PublishResult struct {
	Offset uint64
	// Duplicate is true when MsgID matched an earlier publish; nothing was
	// written and Offset is the original record's.
	Duplicate bool
}

// NewMsgID returns a time-ordered UUIDv7, suitable as a msg id
// (idempotency key). IDs generated later sort after earlier ones.
func NewMsgID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2], b[3], b[4], b[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

// Record is a record read from a stream.
type Record struct {
	Offset uint64
	// Time is the append time (full nanosecond precision).
	Time    time.Time
	Subject string
	// Key is nil when the record has no key.
	Key   []byte
	Value []byte
	// Headers in wire order; a key can repeat. See [Record.Header].
	Headers []Header
}

func recordFromWire(w *proto.WireRecord) Record {
	return Record{
		Offset:  w.Offset,
		Time:    time.Unix(0, int64(w.TimestampNs)),
		Subject: w.Subject,
		Key:     w.Key,
		Value:   w.Value,
		Headers: fromWireHeaders(w.Headers),
	}
}

// Text is the value as a string.
func (r *Record) Text() string { return string(r.Value) }

// JSON unmarshals the value into v.
func (r *Record) JSON(v any) error { return json.Unmarshal(r.Value, v) }

// Header is the value of the first header named name.
func (r *Record) Header(name string) (string, bool) { return findHeader(r.Headers, name) }

// ReadOptions configure [Client.Read]. The zero value reads from offset 0.
type ReadOptions struct {
	// From is the first offset to read.
	From uint64
	// MaxRecords defaults to 100 (the server caps it at 10,000).
	MaxRecords int
	// MaxBytes is the byte budget per response; 0 = server default (1 MiB).
	MaxBytes int
	// Wait long-polls: when caught up, wait up to this long for new records.
	Wait time.Duration
	// Filter is a NATS-style subject filter (orders.*, orders.>); "" = all.
	Filter string
}

// ReadResult is the result of [Client.Read].
type ReadResult struct {
	Records []Record
	// NextOffset is where to continue (pass it as From).
	NextOffset uint64
	// HighWatermark is the stream's next offset at the time of the read.
	HighWatermark uint64
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

// AckPolicy says whether delivered records must be acknowledged.
type AckPolicy string

// Ack policies. The empty value is the server default (explicit).
const (
	// AckExplicit: each record must be acked; unacked records are redelivered.
	AckExplicit AckPolicy = "explicit"
	// AckNone: records count as acked when delivered (at most once).
	AckNone AckPolicy = "none"
)

// HeaderMatch says how a consumer's FilterHeaders combine.
type HeaderMatch string

// Header match modes. The empty value is the server default (all).
const (
	HeaderMatchAll HeaderMatch = "all"
	HeaderMatchAny HeaderMatch = "any"
)

type deliverKind uint8

const (
	deliverUnset deliverKind = iota
	deliverAll
	deliverNew
	deliverFromOffset
	deliverFromTime
)

// DeliverPolicy is where a new consumer starts. The zero value is the
// server default ([DeliverAll]).
type DeliverPolicy struct {
	kind  deliverKind
	value uint64
}

// DeliverAll starts at the first retained record.
func DeliverAll() DeliverPolicy { return DeliverPolicy{kind: deliverAll} }

// DeliverNew delivers only records appended after the consumer is created.
func DeliverNew() DeliverPolicy { return DeliverPolicy{kind: deliverNew} }

// DeliverFromOffset starts at offset.
func DeliverFromOffset(offset uint64) DeliverPolicy {
	return DeliverPolicy{kind: deliverFromOffset, value: offset}
}

// DeliverFromTime starts at the first record at or after t (ms precision).
func DeliverFromTime(t time.Time) DeliverPolicy {
	ms := t.UnixMilli()
	if ms < 0 {
		ms = 0
	}
	return DeliverPolicy{kind: deliverFromTime, value: uint64(ms)}
}

// IsZero reports whether the policy is unset (server default).
func (d DeliverPolicy) IsZero() bool { return d.kind == deliverUnset }

// FromOffset reports the start offset of a DeliverFromOffset policy.
func (d DeliverPolicy) FromOffset() (uint64, bool) { return d.value, d.kind == deliverFromOffset }

// FromTime reports the start time of a DeliverFromTime policy.
func (d DeliverPolicy) FromTime() (time.Time, bool) {
	return time.UnixMilli(int64(d.value)), d.kind == deliverFromTime
}

// String is "all", "new", "from_offset:<n>", "from_time:<ms>" or "" (unset).
func (d DeliverPolicy) String() string {
	switch d.kind {
	case deliverAll:
		return "all"
	case deliverNew:
		return "new"
	case deliverFromOffset:
		return "from_offset:" + strconv.FormatUint(d.value, 10)
	case deliverFromTime:
		return "from_time:" + strconv.FormatUint(d.value, 10)
	}
	return ""
}

func (d DeliverPolicy) wire() any {
	switch d.kind {
	case deliverAll:
		return "all"
	case deliverNew:
		return "new"
	case deliverFromOffset:
		return map[string]uint64{"from_offset": d.value}
	case deliverFromTime:
		return map[string]uint64{"from_time": d.value}
	}
	return nil
}

func parseDeliver(raw json.RawMessage) (DeliverPolicy, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return DeliverPolicy{}, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		switch s {
		case "all":
			return DeliverAll(), nil
		case "new":
			return DeliverNew(), nil
		}
		return DeliverPolicy{}, fmt.Errorf("unknown deliver policy %q", s)
	}
	var m map[string]uint64
	if err := json.Unmarshal(raw, &m); err != nil {
		return DeliverPolicy{}, err
	}
	if v, ok := m["from_offset"]; ok {
		return DeliverFromOffset(v), nil
	}
	if v, ok := m["from_time"]; ok {
		return DeliverPolicy{kind: deliverFromTime, value: v}, nil
	}
	return DeliverPolicy{}, fmt.Errorf("unknown deliver policy %s", raw)
}

// ConsumerSpec is a consumer's settings. Only Name and Stream are
// required; zero values mean "server default".
type ConsumerSpec struct {
	Name   string
	Stream string
	// FilterSubjects are NATS-style subject filters; empty = all subjects.
	FilterSubjects []string
	// Deliver is where the consumer starts; default DeliverAll.
	Deliver DeliverPolicy
	// Ack policy; default AckExplicit.
	Ack AckPolicy
	// AckWait redelivers a record not acked within this time (whole ms);
	// server default 30s.
	AckWait time.Duration
	// MaxDeliver dead-letters after this many deliveries; 0 = server
	// default (5), -1 = never.
	MaxDeliver int
	// Backoff is the redelivery delays by delivery count (the last
	// repeats); empty = redeliver immediately.
	Backoff []time.Duration
	// MaxAckPending pauses delivery while this many records await an ack;
	// 0 = server default (1000), -1 = no limit.
	MaxAckPending int
	// DLQStream receives dead letters; "" = they are dropped (and counted).
	DLQStream string
	// Ephemeral consumers are deleted when the connection that created
	// them closes (the client re-creates them after a reconnect).
	Ephemeral bool
	// DeadLetterExpired dead-letters records whose TTL expires before they
	// are acked (reason "expired") instead of dropping them.
	DeadLetterExpired bool
	// FilterHeaders delivers only records whose headers have these exact
	// values, combined by HeaderMatch.
	FilterHeaders map[string]string
	// HeaderMatch: HeaderMatchAll (default) or HeaderMatchAny.
	HeaderMatch HeaderMatch
	// SingleActive delivers to one subscription at a time (the oldest
	// connected); the next takes over when it goes away. Pulls are refused.
	SingleActive bool
	// PriorityWindow looks this many records ahead and delivers higher
	// priorities first; 0 = strictly in order (server maximum 10,000).
	PriorityWindow int
}

// wireConsumerSpec is the ConsumerSpec JSON the server expects, keys in
// the order of the Rust struct; nil fields are left out (server default).
type wireConsumerSpec struct {
	Name              string            `json:"name"`
	Stream            string            `json:"stream"`
	FilterSubjects    []string          `json:"filter_subjects,omitempty"`
	Deliver           any               `json:"deliver,omitempty"`
	Ack               *string           `json:"ack,omitempty"`
	AckWaitMs         *uint64           `json:"ack_wait_ms,omitempty"`
	MaxDeliver        *uint32           `json:"max_deliver,omitempty"`
	BackoffMs         []uint64          `json:"backoff_ms,omitempty"`
	MaxAckPending     *uint32           `json:"max_ack_pending,omitempty"`
	DLQStream         *string           `json:"dlq_stream,omitempty"`
	Ephemeral         *bool             `json:"ephemeral,omitempty"`
	DeadLetterExpired *bool             `json:"dead_letter_expired,omitempty"`
	FilterHeaders     map[string]string `json:"filter_headers,omitempty"`
	HeaderMatch       *string           `json:"header_match,omitempty"`
	SingleActive      *bool             `json:"single_active,omitempty"`
	PriorityWindow    *uint32           `json:"priority_window,omitempty"`
}

func ptr[T any](v T) *T { return &v }

func countField(name string, v int) (*uint32, error) {
	switch {
	case v == 0:
		return nil, nil
	case v == -1:
		return ptr(uint32(0)), nil
	case v < 0 || int64(v) > 0xffffffff:
		return nil, invalidf("%s out of range: %d", name, v)
	}
	return ptr(uint32(v)), nil
}

func (s *ConsumerSpec) toWire() (*wireConsumerSpec, error) {
	if s.Name == "" {
		return nil, invalidf("consumer name is required")
	}
	if s.Stream == "" {
		return nil, invalidf("consumer stream is required")
	}
	w := &wireConsumerSpec{Name: s.Name, Stream: s.Stream, Deliver: s.Deliver.wire()}
	if len(s.FilterSubjects) > 0 {
		w.FilterSubjects = s.FilterSubjects
	}
	if s.Ack != "" {
		w.Ack = ptr(string(s.Ack))
	}
	if s.AckWait < 0 || s.PriorityWindow < 0 {
		return nil, invalidf("AckWait and PriorityWindow must not be negative")
	}
	if s.AckWait > 0 {
		w.AckWaitMs = ptr(ceilUnits(s.AckWait, time.Millisecond))
	}
	var err error
	if w.MaxDeliver, err = countField("MaxDeliver", s.MaxDeliver); err != nil {
		return nil, err
	}
	if w.MaxAckPending, err = countField("MaxAckPending", s.MaxAckPending); err != nil {
		return nil, err
	}
	for _, b := range s.Backoff {
		if b < 0 {
			return nil, invalidf("backoff must not be negative")
		}
		w.BackoffMs = append(w.BackoffMs, ceilUnits(b, time.Millisecond))
	}
	if s.DLQStream != "" {
		w.DLQStream = ptr(s.DLQStream)
	}
	if s.Ephemeral {
		w.Ephemeral = ptr(true)
	}
	if s.DeadLetterExpired {
		w.DeadLetterExpired = ptr(true)
	}
	if len(s.FilterHeaders) > 0 {
		w.FilterHeaders = s.FilterHeaders
	}
	if s.HeaderMatch != "" {
		w.HeaderMatch = ptr(string(s.HeaderMatch))
	}
	if s.SingleActive {
		w.SingleActive = ptr(true)
	}
	if s.PriorityWindow > 0 {
		w.PriorityWindow = ptr(uint32(s.PriorityWindow))
	}
	return w, nil
}

// MarshalJSON encodes the spec as the server's ConsumerSpec JSON (unset
// fields left out).
func (s ConsumerSpec) MarshalJSON() ([]byte, error) {
	w, err := s.toWire()
	if err != nil {
		return nil, err
	}
	return marshalJSON(w)
}

// UnmarshalJSON decodes the server's ConsumerSpec JSON (as found in
// [ConsumerInfo]). A max_deliver or max_ack_pending of 0 (unlimited)
// becomes -1, so the spec can be passed back to CreateConsumer unchanged.
func (s *ConsumerSpec) UnmarshalJSON(b []byte) error {
	var raw struct {
		Name              string            `json:"name"`
		Stream            string            `json:"stream"`
		FilterSubjects    []string          `json:"filter_subjects"`
		Deliver           json.RawMessage   `json:"deliver"`
		Ack               string            `json:"ack"`
		AckWaitMs         *uint64           `json:"ack_wait_ms"`
		MaxDeliver        *uint32           `json:"max_deliver"`
		BackoffMs         []uint64          `json:"backoff_ms"`
		MaxAckPending     *uint32           `json:"max_ack_pending"`
		DLQStream         *string           `json:"dlq_stream"`
		Ephemeral         bool              `json:"ephemeral"`
		DeadLetterExpired bool              `json:"dead_letter_expired"`
		FilterHeaders     map[string]string `json:"filter_headers"`
		HeaderMatch       string            `json:"header_match"`
		SingleActive      bool              `json:"single_active"`
		PriorityWindow    uint32            `json:"priority_window"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	deliver, err := parseDeliver(raw.Deliver)
	if err != nil {
		return err
	}
	count := func(p *uint32) int {
		switch {
		case p == nil:
			return 0
		case *p == 0:
			return -1
		}
		return int(*p)
	}
	*s = ConsumerSpec{
		Name:              raw.Name,
		Stream:            raw.Stream,
		FilterSubjects:    raw.FilterSubjects,
		Deliver:           deliver,
		Ack:               AckPolicy(raw.Ack),
		MaxDeliver:        count(raw.MaxDeliver),
		MaxAckPending:     count(raw.MaxAckPending),
		Ephemeral:         raw.Ephemeral,
		DeadLetterExpired: raw.DeadLetterExpired,
		FilterHeaders:     raw.FilterHeaders,
		HeaderMatch:       HeaderMatch(raw.HeaderMatch),
		SingleActive:      raw.SingleActive,
		PriorityWindow:    int(raw.PriorityWindow),
	}
	if raw.AckWaitMs != nil {
		s.AckWait = time.Duration(*raw.AckWaitMs) * time.Millisecond
	}
	for _, ms := range raw.BackoffMs {
		s.Backoff = append(s.Backoff, time.Duration(ms)*time.Millisecond)
	}
	if raw.DLQStream != nil {
		s.DLQStream = *raw.DLQStream
	}
	return nil
}

// ConsumerInfo is a consumer's spec, position and counters.
type ConsumerInfo struct {
	Spec ConsumerSpec `json:"spec"`
	// NextOffset is the next stream offset to be delivered for the first time.
	NextOffset uint64 `json:"next_offset"`
	// AckFloor: everything below this offset is acked (or filtered out).
	AckFloor    uint64 `json:"ack_floor"`
	NumUnacked  uint64 `json:"num_unacked"`
	NumInFlight uint64 `json:"num_in_flight"`
	// NumDelayed counts records held back until their delivery time.
	NumDelayed  uint64        `json:"num_delayed"`
	NumWaiting  uint64        `json:"num_waiting"`
	Lag         uint64        `json:"lag"`
	Subscribers uint64        `json:"subscribers"`
	PullWaiters uint64        `json:"pull_waiters"`
	Stats       ConsumerStats `json:"stats"`
	// Raw is the JSON the server sent.
	Raw json.RawMessage `json:"-"`
}

// ConsumerStats are a consumer's lifetime counters.
type ConsumerStats struct {
	Delivered    uint64 `json:"delivered"`
	Redelivered  uint64 `json:"redelivered"`
	Acked        uint64 `json:"acked"`
	DeadLettered uint64 `json:"dead_lettered"`
	Gone         uint64 `json:"gone"`
	Skipped      uint64 `json:"skipped"`
}

// SeekTarget is where [Client.Seek] moves a consumer's cursor.
type SeekTarget struct {
	kind  byte
	value uint64
}

// SeekEarliest moves to the first retained record.
func SeekEarliest() SeekTarget { return SeekTarget{kind: proto.SeekEarliest} }

// SeekLatest moves to the end of the stream.
func SeekLatest() SeekTarget { return SeekTarget{kind: proto.SeekLatest} }

// SeekOffset moves to offset.
func SeekOffset(offset uint64) SeekTarget { return SeekTarget{kind: proto.SeekOffset, value: offset} }

// SeekTime moves to the first record at or after t (ms precision).
func SeekTime(t time.Time) SeekTarget {
	ms := t.UnixMilli()
	if ms < 0 {
		ms = 0
	}
	return SeekTarget{kind: proto.SeekTime, value: uint64(ms)}
}

// SubscribeOptions configure [Client.Subscribe].
type SubscribeOptions struct {
	// Window is the credit window: how many records the server may push
	// ahead of the application. The client returns credit as messages are
	// taken. Default 256.
	Window int
}

// PullOptions configure [Client.Pull].
type PullOptions struct {
	// MaxMessages defaults to 100.
	MaxMessages int
	// MaxBytes is the byte budget; 0 = server default.
	MaxBytes int
	// Expires is how long to wait for at least one message; default 5s.
	Expires time.Duration
	// NoWait returns at once with whatever is available (Expires is ignored).
	NoWait bool
}

// QueryResult is the result of a bounded ExQL query.
type QueryResult struct {
	Columns []string `json:"columns"`
	// Rows hold JSON values; numbers decode as json.Number.
	Rows            [][]any `json:"rows"`
	RowCount        int     `json:"row_count"`
	ExecutionTimeMs uint64  `json:"execution_time_ms"`
	// Truncated is true when more rows existed than the server returns.
	Truncated bool `json:"truncated"`
}

// marshalJSON encodes v without HTML escaping (so "orders.>" stays as is,
// like serde_json).
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
