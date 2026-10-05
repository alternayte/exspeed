package proto

// Message is any request or response.
type Message interface {
	// Opcode is the frame opcode of this message.
	Opcode() byte
	encode(w *Writer)
}

// ---------------------------------------------------------------------------
// Shared structures
// ---------------------------------------------------------------------------

// PublishRecord is a record as published: str subject, opt<bytes> key,
// bytes value, headers, opt<str> msg_id.
type PublishRecord struct {
	Subject string
	Key     []byte // nil = absent
	Value   []byte
	Headers []Header
	MsgID   *string
}

func (w *Writer) publishRecord(r *PublishRecord) {
	w.Str(r.Subject)
	w.OptBytes(r.Key)
	w.Bytes(r.Value)
	w.Headers(r.Headers)
	w.OptStr(r.MsgID)
}

func (r *Reader) publishRecord() (PublishRecord, error) {
	var p PublishRecord
	var err error
	if p.Subject, err = r.Str(); err != nil {
		return p, err
	}
	if p.Key, err = r.OptBytes(); err != nil {
		return p, err
	}
	if p.Value, err = r.Bytes(); err != nil {
		return p, err
	}
	if p.Headers, err = r.Headers(); err != nil {
		return p, err
	}
	p.MsgID, err = r.OptStr()
	return p, err
}

// StreamSpec is str name, u64 max_age_secs, u64 max_bytes, u64
// dedup_window_secs, u64 dedup_max_entries, u8 compaction, then the limits
// JSON as bytes when present.
type StreamSpec struct {
	Name            string
	MaxAgeSecs      uint64
	MaxBytes        uint64
	DedupWindowSecs uint64
	DedupMaxEntries uint64
	Compaction      bool
	// Limits is the StreamLimits JSON object, or nil when every limit is at
	// its default (then nothing is sent).
	Limits []byte
}

func (w *Writer) streamSpec(s *StreamSpec) {
	w.Str(s.Name)
	w.U64(s.MaxAgeSecs)
	w.U64(s.MaxBytes)
	w.U64(s.DedupWindowSecs)
	w.U64(s.DedupMaxEntries)
	w.Bool(s.Compaction)
	if s.Limits != nil {
		w.Bytes(s.Limits)
	}
}

func (r *Reader) streamSpec() (StreamSpec, error) {
	var s StreamSpec
	var err error
	if s.Name, err = r.Str(); err != nil {
		return s, err
	}
	for _, p := range []*uint64{&s.MaxAgeSecs, &s.MaxBytes, &s.DedupWindowSecs, &s.DedupMaxEntries} {
		if *p, err = r.U64(); err != nil {
			return s, err
		}
	}
	if s.Compaction, err = r.Bool(); err != nil {
		return s, err
	}
	if r.Remaining() > 0 {
		s.Limits, err = r.Bytes()
	}
	return s, err
}

func (w *Writer) offsets(o []uint64) {
	w.U32(uint32(len(o)))
	for _, v := range o {
		w.U64(v)
	}
}

func (r *Reader) offsets() ([]uint64, error) {
	n, err := r.Count(8)
	if err != nil {
		return nil, err
	}
	out := make([]uint64, n)
	for i := range out {
		if out[i], err = r.U64(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

// Connect is the handshake: str client_id, opt<str> token.
type Connect struct {
	ClientID string
	Token    *string
}

// Ping asks for a Pong.
type Ping struct{}

// Metadata asks for node id, leadership and server version.
type Metadata struct{}

// Publish appends one record.
type Publish struct {
	Stream string
	Record PublishRecord
}

// PublishBatch appends several records.
type PublishBatch struct {
	Stream  string
	Records []PublishRecord
}

// CreateStream creates a stream.
type CreateStream struct{ Spec StreamSpec }

// UpdateStream replaces a stream's settings.
type UpdateStream struct{ Spec StreamSpec }

// DeleteStream deletes a stream.
type DeleteStream struct{ Name string }

// StreamInfo asks for a stream's info (JSON).
type StreamInfo struct{ Name string }

// ListStreams lists streams (JSON array).
type ListStreams struct{}

// Query runs a bounded ExQL query: lstr sql.
type Query struct{ SQL string }

// CreateConsumer carries the ConsumerSpec JSON as bytes.
type CreateConsumer struct{ Spec []byte }

// DeleteConsumer deletes a consumer.
type DeleteConsumer struct{ Name string }

// ConsumerInfo asks for a consumer's info (JSON).
type ConsumerInfo struct{ Name string }

// ListConsumers lists consumers, optionally of one stream.
type ListConsumers struct{ Stream *string }

// SeekConsumer moves a consumer's cursor.
type SeekConsumer struct {
	Consumer string
	Kind     byte
	Value    uint64
}

// Subscribe starts push delivery with a credit window.
type Subscribe struct {
	Consumer string
	Credits  uint32
}

// Credit returns credit to a subscription.
type Credit struct {
	SubID   uint32
	Credits uint32
}

// Unsubscribe ends a subscription (consumer or core).
type Unsubscribe struct{ SubID uint32 }

// Pull fetches a batch from a consumer.
type Pull struct {
	Consumer    string
	MaxMessages uint32
	MaxBytes    uint32
	ExpiresMs   uint32
}

// Ack acknowledges offsets.
type Ack struct {
	Consumer string
	Offsets  []uint64
}

// Nack asks for redelivery after DelayMs (0 = the consumer's backoff).
type Nack struct {
	Consumer string
	Offset   uint64
	DelayMs  uint32
}

// Term dead-letters a record now.
type Term struct {
	Consumer string
	Offset   uint64
	Reason   string
}

// InProgress resets ack deadlines.
type InProgress struct {
	Consumer string
	Offsets  []uint64
}

// Read is a stateless read of a stream.
type Read struct {
	Stream     string
	From       uint64
	MaxRecords uint32
	MaxBytes   uint32
	WaitMs     uint32
	Filter     string
}

// CorePublish publishes a core message.
type CorePublish struct {
	Subject string
	ReplyTo *string
	Headers []Header
	Value   []byte
}

// CoreSubscribe subscribes to core messages.
type CoreSubscribe struct {
	Subject string
	Queue   *string
}

// KvCreateBucket creates a key-value bucket.
type KvCreateBucket struct {
	Bucket   string
	History  uint64
	TTLMs    uint64
	MaxBytes uint64
}

// KvPut sets a key.
type KvPut struct {
	Bucket           string
	Key              string
	Value            []byte
	ExpectedRevision *uint64
	TTLMs            *uint64
}

// KvGet reads a key.
type KvGet struct {
	Bucket   string
	Key      string
	Revision *uint64
}

// KvDelete deletes or purges a key.
type KvDelete struct {
	Bucket           string
	Key              string
	Purge            bool
	ExpectedRevision *uint64
}

// KvKeys lists keys matching a filter.
type KvKeys struct {
	Bucket string
	Filter string
}

// KvHistory lists a key's kept revisions.
type KvHistory struct {
	Bucket string
	Key    string
}

func (Connect) Opcode() byte        { return OpConnect }
func (Ping) Opcode() byte           { return OpPing }
func (Metadata) Opcode() byte       { return OpMetadata }
func (Publish) Opcode() byte        { return OpPublish }
func (PublishBatch) Opcode() byte   { return OpPublishBatch }
func (CreateStream) Opcode() byte   { return OpCreateStream }
func (UpdateStream) Opcode() byte   { return OpUpdateStream }
func (DeleteStream) Opcode() byte   { return OpDeleteStream }
func (StreamInfo) Opcode() byte     { return OpStreamInfo }
func (ListStreams) Opcode() byte    { return OpListStreams }
func (Query) Opcode() byte          { return OpQuery }
func (CreateConsumer) Opcode() byte { return OpCreateConsumer }
func (DeleteConsumer) Opcode() byte { return OpDeleteConsumer }
func (ConsumerInfo) Opcode() byte   { return OpConsumerInfo }
func (ListConsumers) Opcode() byte  { return OpListConsumers }
func (SeekConsumer) Opcode() byte   { return OpSeekConsumer }
func (Subscribe) Opcode() byte      { return OpSubscribe }
func (Credit) Opcode() byte         { return OpCredit }
func (Unsubscribe) Opcode() byte    { return OpUnsubscribe }
func (Pull) Opcode() byte           { return OpPull }
func (Ack) Opcode() byte            { return OpAck }
func (Nack) Opcode() byte           { return OpNack }
func (Term) Opcode() byte           { return OpTerm }
func (InProgress) Opcode() byte     { return OpInProgress }
func (Read) Opcode() byte           { return OpRead }
func (CorePublish) Opcode() byte    { return OpCorePublish }
func (CoreSubscribe) Opcode() byte  { return OpCoreSubscribe }
func (KvCreateBucket) Opcode() byte { return OpKvCreateBucket }
func (KvPut) Opcode() byte          { return OpKvPut }
func (KvGet) Opcode() byte          { return OpKvGet }
func (KvDelete) Opcode() byte       { return OpKvDelete }
func (KvKeys) Opcode() byte         { return OpKvKeys }
func (KvHistory) Opcode() byte      { return OpKvHistory }

func (m Connect) encode(w *Writer) { w.Str(m.ClientID); w.OptStr(m.Token) }
func (Ping) encode(*Writer)        {}
func (Metadata) encode(*Writer)    {}
func (m Publish) encode(w *Writer) { w.Str(m.Stream); w.publishRecord(&m.Record) }
func (m PublishBatch) encode(w *Writer) {
	w.Str(m.Stream)
	w.U32(uint32(len(m.Records)))
	for i := range m.Records {
		w.publishRecord(&m.Records[i])
	}
}
func (m CreateStream) encode(w *Writer)   { w.streamSpec(&m.Spec) }
func (m UpdateStream) encode(w *Writer)   { w.streamSpec(&m.Spec) }
func (m DeleteStream) encode(w *Writer)   { w.Str(m.Name) }
func (m StreamInfo) encode(w *Writer)     { w.Str(m.Name) }
func (ListStreams) encode(*Writer)        {}
func (m Query) encode(w *Writer)          { w.LStr(m.SQL) }
func (m CreateConsumer) encode(w *Writer) { w.Bytes(m.Spec) }
func (m DeleteConsumer) encode(w *Writer) { w.Str(m.Name) }
func (m ConsumerInfo) encode(w *Writer)   { w.Str(m.Name) }
func (m ListConsumers) encode(w *Writer)  { w.OptStr(m.Stream) }
func (m SeekConsumer) encode(w *Writer)   { w.Str(m.Consumer); w.U8(m.Kind); w.U64(m.Value) }
func (m Subscribe) encode(w *Writer)      { w.Str(m.Consumer); w.U32(m.Credits) }
func (m Credit) encode(w *Writer)         { w.U32(m.SubID); w.U32(m.Credits) }
func (m Unsubscribe) encode(w *Writer)    { w.U32(m.SubID) }
func (m Pull) encode(w *Writer) {
	w.Str(m.Consumer)
	w.U32(m.MaxMessages)
	w.U32(m.MaxBytes)
	w.U32(m.ExpiresMs)
}
func (m Ack) encode(w *Writer)        { w.Str(m.Consumer); w.offsets(m.Offsets) }
func (m Nack) encode(w *Writer)       { w.Str(m.Consumer); w.U64(m.Offset); w.U32(m.DelayMs) }
func (m Term) encode(w *Writer)       { w.Str(m.Consumer); w.U64(m.Offset); w.Str(m.Reason) }
func (m InProgress) encode(w *Writer) { w.Str(m.Consumer); w.offsets(m.Offsets) }
func (m Read) encode(w *Writer) {
	w.Str(m.Stream)
	w.U64(m.From)
	w.U32(m.MaxRecords)
	w.U32(m.MaxBytes)
	w.U32(m.WaitMs)
	w.Str(m.Filter)
}
func (m CorePublish) encode(w *Writer) {
	w.Str(m.Subject)
	w.OptStr(m.ReplyTo)
	w.Headers(m.Headers)
	w.Bytes(m.Value)
}
func (m CoreSubscribe) encode(w *Writer) { w.Str(m.Subject); w.OptStr(m.Queue) }
func (m KvCreateBucket) encode(w *Writer) {
	w.Str(m.Bucket)
	w.U64(m.History)
	w.U64(m.TTLMs)
	w.U64(m.MaxBytes)
}
func (m KvPut) encode(w *Writer) {
	w.Str(m.Bucket)
	w.Str(m.Key)
	w.Bytes(m.Value)
	w.OptU64(m.ExpectedRevision)
	w.OptU64(m.TTLMs)
}
func (m KvGet) encode(w *Writer) { w.Str(m.Bucket); w.Str(m.Key); w.OptU64(m.Revision) }
func (m KvDelete) encode(w *Writer) {
	w.Str(m.Bucket)
	w.Str(m.Key)
	w.Bool(m.Purge)
	w.OptU64(m.ExpectedRevision)
}
func (m KvKeys) encode(w *Writer)    { w.Str(m.Bucket); w.Str(m.Filter) }
func (m KvHistory) encode(w *Writer) { w.Str(m.Bucket); w.Str(m.Key) }

// ---------------------------------------------------------------------------
// Responses and pushes
// ---------------------------------------------------------------------------

// Ok is an empty success reply.
type Ok struct{}

// Pong answers Ping.
type Pong struct{}

// Error is an error reply: u16 code, str message, opt<bytes> detail (JSON).
type Error struct {
	Code    uint16
	Message string
	Detail  []byte // nil = absent
}

// ConnectOk answers Connect.
type ConnectOk struct {
	ServerVersion string
	NodeID        string
	Leader        *string
}

// PublishOk answers Publish (and KV writes: offset = the new revision).
type PublishOk struct {
	Offset    uint64
	Duplicate bool
}

// PublishResult is one entry of PublishBatchOk.
type PublishResult struct {
	Offset    uint64
	Duplicate bool
}

// PublishBatchOk answers PublishBatch.
type PublishBatchOk struct{ Results []PublishResult }

// SubscribeOk answers Subscribe and CoreSubscribe.
type SubscribeOk struct{ SubID uint32 }

// Deliver is a push of records for a subscription (correlation id 0).
type Deliver struct {
	SubID   uint32
	Records []WireRecord
}

// SubscriptionEnded is a push: the subscription ended server-side.
type SubscriptionEnded struct {
	SubID   uint32
	Code    uint16
	Message string
}

// Messages answers Pull, KvGet and KvHistory.
type Messages struct{ Records []WireRecord }

// ReadResult answers Read.
type ReadResult struct {
	NextOffset    uint64
	HighWatermark uint64
	Records       []WireRecord
}

// JSON is a raw UTF-8 JSON reply.
type JSON struct{ Data []byte }

// CoreMsg is a push of a core message (correlation id 0).
type CoreMsg struct {
	SubID   uint32
	Subject string
	ReplyTo *string
	Headers []Header
	Value   []byte
}

func (Ok) Opcode() byte                { return OpOk }
func (Pong) Opcode() byte              { return OpPong }
func (Error) Opcode() byte             { return OpError }
func (ConnectOk) Opcode() byte         { return OpConnectOk }
func (PublishOk) Opcode() byte         { return OpPublishOk }
func (PublishBatchOk) Opcode() byte    { return OpPublishBatchOk }
func (SubscribeOk) Opcode() byte       { return OpSubscribeOk }
func (Deliver) Opcode() byte           { return OpDeliver }
func (SubscriptionEnded) Opcode() byte { return OpSubscriptionEnded }
func (Messages) Opcode() byte          { return OpMessages }
func (ReadResult) Opcode() byte        { return OpReadResult }
func (JSON) Opcode() byte              { return OpJSON }
func (CoreMsg) Opcode() byte           { return OpCoreMsg }

func (Ok) encode(*Writer)   {}
func (Pong) encode(*Writer) {}
func (m Error) encode(w *Writer) {
	w.U16(m.Code)
	w.Str(m.Message)
	w.OptBytes(m.Detail)
}
func (m ConnectOk) encode(w *Writer) { w.Str(m.ServerVersion); w.Str(m.NodeID); w.OptStr(m.Leader) }
func (m PublishOk) encode(w *Writer) { w.U64(m.Offset); w.Bool(m.Duplicate) }
func (m PublishBatchOk) encode(w *Writer) {
	w.U32(uint32(len(m.Results)))
	for _, r := range m.Results {
		w.U64(r.Offset)
		w.Bool(r.Duplicate)
	}
}
func (m SubscribeOk) encode(w *Writer) { w.U32(m.SubID) }
func (m Deliver) encode(w *Writer)     { w.U32(m.SubID); w.Records(m.Records) }
func (m SubscriptionEnded) encode(w *Writer) {
	w.U32(m.SubID)
	w.U16(m.Code)
	w.Str(m.Message)
}
func (m Messages) encode(w *Writer) { w.Records(m.Records) }
func (m ReadResult) encode(w *Writer) {
	w.U64(m.NextOffset)
	w.U64(m.HighWatermark)
	w.Records(m.Records)
}
func (m JSON) encode(w *Writer) { w.Raw(m.Data) }
func (m CoreMsg) encode(w *Writer) {
	w.U32(m.SubID)
	w.Str(m.Subject)
	w.OptStr(m.ReplyTo)
	w.Headers(m.Headers)
	w.Bytes(m.Value)
}

// ---------------------------------------------------------------------------
// Encoding and decoding entry points
// ---------------------------------------------------------------------------

// Encode returns the payload of m.
func Encode(m Message) ([]byte, error) {
	w := NewWriter(64)
	m.encode(w)
	return w.Finish()
}

// EncodeFrameOf returns the complete frame (header and payload) of m.
func EncodeFrameOf(m Message, corr uint32) ([]byte, error) {
	w := frameWriter(64)
	m.encode(w)
	return finishFrame(w, m.Opcode(), corr)
}

// DecodeResponse decodes a server response or push.
func DecodeResponse(opcode byte, payload []byte) (Message, error) {
	if opcode == OpJSON {
		return JSON{Data: payload}, nil
	}
	r := NewReader(payload)
	var m Message
	var err error
	switch opcode {
	case OpOk:
		m = Ok{}
	case OpPong:
		m = Pong{}
	case OpError:
		var e Error
		if e.Code, err = r.U16(); err != nil {
			return nil, err
		}
		if e.Message, err = r.Str(); err != nil {
			return nil, err
		}
		if e.Detail, err = r.OptBytes(); err != nil {
			return nil, err
		}
		m = e
	case OpConnectOk:
		var c ConnectOk
		if c.ServerVersion, err = r.Str(); err != nil {
			return nil, err
		}
		if c.NodeID, err = r.Str(); err != nil {
			return nil, err
		}
		if c.Leader, err = r.OptStr(); err != nil {
			return nil, err
		}
		m = c
	case OpPublishOk:
		var p PublishOk
		if p.Offset, err = r.U64(); err != nil {
			return nil, err
		}
		if p.Duplicate, err = r.Bool(); err != nil {
			return nil, err
		}
		m = p
	case OpPublishBatchOk:
		n, err := r.Count(9)
		if err != nil {
			return nil, err
		}
		res := make([]PublishResult, n)
		for i := range res {
			if res[i].Offset, err = r.U64(); err != nil {
				return nil, err
			}
			if res[i].Duplicate, err = r.Bool(); err != nil {
				return nil, err
			}
		}
		m = PublishBatchOk{Results: res}
	case OpSubscribeOk:
		var s SubscribeOk
		if s.SubID, err = r.U32(); err != nil {
			return nil, err
		}
		m = s
	case OpCoreMsg:
		var c CoreMsg
		if c.SubID, err = r.U32(); err != nil {
			return nil, err
		}
		if c.Subject, err = r.Str(); err != nil {
			return nil, err
		}
		if c.ReplyTo, err = r.OptStr(); err != nil {
			return nil, err
		}
		if c.Headers, err = r.Headers(); err != nil {
			return nil, err
		}
		if c.Value, err = r.Bytes(); err != nil {
			return nil, err
		}
		m = c
	case OpDeliver:
		var d Deliver
		if d.SubID, err = r.U32(); err != nil {
			return nil, err
		}
		if d.Records, err = r.Records(); err != nil {
			return nil, err
		}
		m = d
	case OpSubscriptionEnded:
		var s SubscriptionEnded
		if s.SubID, err = r.U32(); err != nil {
			return nil, err
		}
		if s.Code, err = r.U16(); err != nil {
			return nil, err
		}
		if s.Message, err = r.Str(); err != nil {
			return nil, err
		}
		m = s
	case OpMessages:
		recs, err := r.Records()
		if err != nil {
			return nil, err
		}
		m = Messages{Records: recs}
	case OpReadResult:
		var rr ReadResult
		if rr.NextOffset, err = r.U64(); err != nil {
			return nil, err
		}
		if rr.HighWatermark, err = r.U64(); err != nil {
			return nil, err
		}
		if rr.Records, err = r.Records(); err != nil {
			return nil, err
		}
		m = rr
	default:
		return nil, decodeErr("opcode 0x%02x is not a server response", opcode)
	}
	if err := r.Finish(); err != nil {
		return nil, err
	}
	return m, nil
}

// DecodeRequest decodes a client request (used by test servers).
func DecodeRequest(opcode byte, payload []byte) (Message, error) {
	r := NewReader(payload)
	m, err := decodeRequest(r, opcode)
	if err != nil {
		return nil, err
	}
	if err := r.Finish(); err != nil {
		return nil, err
	}
	return m, nil
}

func decodeRequest(r *Reader, opcode byte) (Message, error) {
	var err error
	str := func() string {
		if err != nil {
			return ""
		}
		var s string
		s, err = r.Str()
		return s
	}
	u32 := func() uint32 {
		if err != nil {
			return 0
		}
		var v uint32
		v, err = r.U32()
		return v
	}
	u64 := func() uint64 {
		if err != nil {
			return 0
		}
		var v uint64
		v, err = r.U64()
		return v
	}
	optStr := func() *string {
		if err != nil {
			return nil
		}
		var v *string
		v, err = r.OptStr()
		return v
	}
	optU64 := func() *uint64 {
		if err != nil {
			return nil
		}
		var v *uint64
		v, err = r.OptU64()
		return v
	}
	bytes := func() []byte {
		if err != nil {
			return nil
		}
		var v []byte
		v, err = r.Bytes()
		return v
	}
	var m Message
	switch opcode {
	case OpConnect:
		m = Connect{ClientID: str(), Token: optStr()}
	case OpPing:
		m = Ping{}
	case OpMetadata:
		m = Metadata{}
	case OpPublish:
		p := Publish{Stream: str()}
		if err == nil {
			p.Record, err = r.publishRecord()
		}
		m = p
	case OpPublishBatch:
		p := PublishBatch{Stream: str()}
		if err != nil {
			return nil, err
		}
		n, err := r.Count(10)
		if err != nil {
			return nil, err
		}
		p.Records = make([]PublishRecord, n)
		for i := range p.Records {
			if p.Records[i], err = r.publishRecord(); err != nil {
				return nil, err
			}
		}
		m = p
	case OpCreateStream, OpUpdateStream:
		s, e := r.streamSpec()
		err = e
		if opcode == OpCreateStream {
			m = CreateStream{Spec: s}
		} else {
			m = UpdateStream{Spec: s}
		}
	case OpDeleteStream:
		m = DeleteStream{Name: str()}
	case OpStreamInfo:
		m = StreamInfo{Name: str()}
	case OpListStreams:
		m = ListStreams{}
	case OpQuery:
		var s string
		s, err = r.LStr()
		m = Query{SQL: s}
	case OpCreateConsumer:
		m = CreateConsumer{Spec: bytes()}
	case OpDeleteConsumer:
		m = DeleteConsumer{Name: str()}
	case OpConsumerInfo:
		m = ConsumerInfo{Name: str()}
	case OpListConsumers:
		m = ListConsumers{Stream: optStr()}
	case OpSeekConsumer:
		s := SeekConsumer{Consumer: str()}
		if err == nil {
			s.Kind, err = r.U8()
		}
		s.Value = u64()
		if err == nil && s.Kind > SeekTime {
			err = decodeErr("unknown seek kind %d", s.Kind)
		}
		m = s
	case OpSubscribe:
		m = Subscribe{Consumer: str(), Credits: u32()}
	case OpCredit:
		m = Credit{SubID: u32(), Credits: u32()}
	case OpUnsubscribe:
		m = Unsubscribe{SubID: u32()}
	case OpPull:
		m = Pull{Consumer: str(), MaxMessages: u32(), MaxBytes: u32(), ExpiresMs: u32()}
	case OpAck, OpInProgress:
		c := str()
		var o []uint64
		if err == nil {
			o, err = r.offsets()
		}
		if opcode == OpAck {
			m = Ack{Consumer: c, Offsets: o}
		} else {
			m = InProgress{Consumer: c, Offsets: o}
		}
	case OpNack:
		m = Nack{Consumer: str(), Offset: u64(), DelayMs: u32()}
	case OpTerm:
		m = Term{Consumer: str(), Offset: u64(), Reason: str()}
	case OpRead:
		m = Read{Stream: str(), From: u64(), MaxRecords: u32(), MaxBytes: u32(), WaitMs: u32(), Filter: str()}
	case OpCorePublish:
		c := CorePublish{Subject: str(), ReplyTo: optStr()}
		if err == nil {
			c.Headers, err = r.Headers()
		}
		c.Value = bytes()
		m = c
	case OpCoreSubscribe:
		m = CoreSubscribe{Subject: str(), Queue: optStr()}
	case OpKvCreateBucket:
		m = KvCreateBucket{Bucket: str(), History: u64(), TTLMs: u64(), MaxBytes: u64()}
	case OpKvPut:
		m = KvPut{Bucket: str(), Key: str(), Value: bytes(), ExpectedRevision: optU64(), TTLMs: optU64()}
	case OpKvGet:
		m = KvGet{Bucket: str(), Key: str(), Revision: optU64()}
	case OpKvDelete:
		d := KvDelete{Bucket: str(), Key: str()}
		if err == nil {
			d.Purge, err = r.Bool()
		}
		d.ExpectedRevision = optU64()
		m = d
	case OpKvKeys:
		m = KvKeys{Bucket: str(), Filter: str()}
	case OpKvHistory:
		m = KvHistory{Bucket: str(), Key: str()}
	default:
		return nil, decodeErr("opcode 0x%02x is not a client request", opcode)
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}
