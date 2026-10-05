package proto

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// unhex turns a hex string (spaces, newlines and '|' ignored) into bytes.
func unhex(t *testing.T, s string) []byte {
	t.Helper()
	clean := strings.NewReplacer(" ", "", "\n", "", "\t", "", "|", "").Replace(s)
	b, err := hex.DecodeString(clean)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func sp(s string) *string   { return &s }
func u64p(v uint64) *uint64 { return &v }

// rec is the same as rec(i) in the Rust protocol tests.
func rec(i uint64) WireRecord {
	r := WireRecord{
		Offset:        i,
		TimestampNs:   1_700_000_000_000_000_000 + i,
		DeliveryCount: uint16(i % 3),
		Subject:       fmt.Sprintf("orders.%d", i),
		Value:         bytes.Repeat([]byte{byte(i)}, int(i%7)),
		Headers:       []Header{{Key: "h", Value: fmt.Sprintf("v%d", i)}},
	}
	if i%2 == 0 {
		r.Key = []byte(fmt.Sprintf("k%d", i))
	}
	return r
}

var pr = PublishRecord{
	Subject: "a.b",
	Key:     []byte("k"),
	Value:   []byte(`{"x":1}`),
	Headers: []Header{{Key: "h1", Value: "v1"}},
	MsgID:   sp("m-1"),
}

var emptyPr = PublishRecord{Value: []byte{}}

var spec = StreamSpec{Name: "s", MaxAgeSecs: 1, MaxBytes: 2, DedupWindowSecs: 3, DedupMaxEntries: 4, Compaction: true}

// limitsJSON is serde_json's encoding of the `limited` spec in the Rust
// round-trip test.
const limitsJSON = `{"max_msgs":10,"discard":"new","max_msgs_per_subject":1,"allow_msg_ttl":true,` +
	`"msg_ttl_ms":5000,"allow_delayed":true,"retention":"work_queue"}`

var limitedSpec = StreamSpec{Name: "q", Limits: []byte(limitsJSON)}

// consumerJSON is serde_json::to_vec(&spec) of the consumer spec in the
// Rust round-trip test.
const consumerJSON = `{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},` +
	`"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],` +
	`"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,` +
	`"dead_letter_expired":false,"header_match":"all","single_active":false,"priority_window":0}`

func allRequests() []Message {
	return []Message{
		Connect{ClientID: "c", Token: sp("t")},
		Connect{ClientID: "c"},
		Ping{},
		Metadata{},
		Publish{Stream: "s", Record: pr},
		PublishBatch{Stream: "s", Records: []PublishRecord{pr, emptyPr}},
		CreateStream{Spec: spec},
		UpdateStream{Spec: spec},
		CreateStream{Spec: limitedSpec},
		DeleteStream{Name: "s"},
		StreamInfo{Name: "s"},
		ListStreams{},
		Query{SQL: "SELECT 1"},
		CreateConsumer{Spec: []byte(consumerJSON)},
		DeleteConsumer{Name: "c"},
		ConsumerInfo{Name: "c"},
		ListConsumers{},
		ListConsumers{Stream: sp("s")},
		SeekConsumer{Consumer: "c", Kind: SeekTime, Value: 9},
		SeekConsumer{Consumer: "c", Kind: SeekLatest},
		Subscribe{Consumer: "c", Credits: 100},
		Credit{SubID: 3, Credits: 10},
		Unsubscribe{SubID: 3},
		Pull{Consumer: "c", MaxMessages: 10, MaxBytes: 1024, ExpiresMs: 500},
		Ack{Consumer: "c", Offsets: []uint64{1, 2, 3}},
		Nack{Consumer: "c", Offset: 4, DelayMs: 100},
		Term{Consumer: "c", Offset: 5, Reason: "bad"},
		InProgress{Consumer: "c", Offsets: []uint64{6}},
		Read{Stream: "s", From: 7, MaxRecords: 100, MaxBytes: 1 << 20, WaitMs: 1000, Filter: "a.*"},
		CorePublish{Subject: "a.b", ReplyTo: sp("r"), Headers: []Header{{Key: "h", Value: "v"}}, Value: []byte("x")},
		CorePublish{Subject: "a", Value: []byte{}},
		CoreSubscribe{Subject: "a.*", Queue: sp("q")},
		CoreSubscribe{Subject: "a.*"},
		KvCreateBucket{Bucket: "b", History: 5, TTLMs: 1000},
		KvPut{Bucket: "b", Key: "k", Value: []byte("v"), ExpectedRevision: u64p(0)},
		KvPut{Bucket: "b", Key: "k", Value: []byte("v"), TTLMs: u64p(500)},
		KvGet{Bucket: "b", Key: "k", Revision: u64p(3)},
		KvGet{Bucket: "b", Key: "k"},
		KvDelete{Bucket: "b", Key: "k", Purge: true, ExpectedRevision: u64p(7)},
		KvDelete{Bucket: "b", Key: "k"},
		KvKeys{Bucket: "b", Filter: "a.*"},
		KvHistory{Bucket: "b", Key: "k"},
	}
}

func allResponses() []Message {
	return []Message{
		Ok{},
		Pong{},
		Error{Code: 404, Message: "nope"},
		Error{Code: 503, Message: "not leader", Detail: []byte(`{"leader":"h:1"}`)},
		ConnectOk{ServerVersion: "0.6.0", NodeID: "n1", Leader: sp("h:5933")},
		PublishOk{Offset: 9, Duplicate: true},
		PublishBatchOk{Results: []PublishResult{{Offset: 1}, {Offset: 1, Duplicate: true}}},
		SubscribeOk{SubID: 2},
		Deliver{SubID: 2, Records: []WireRecord{rec(0), rec(1), rec(2), rec(3), rec(4)}},
		SubscriptionEnded{SubID: 2, Code: 404, Message: "consumer deleted"},
		Messages{Records: []WireRecord{rec(1)}},
		ReadResult{NextOffset: 10, HighWatermark: 12, Records: []WireRecord{rec(0), rec(1), rec(2)}},
		JSON{Data: []byte(`{"a":1}`)},
		CoreMsg{SubID: 0x80000001, Subject: "a", ReplyTo: sp("r"), Headers: []Header{{Key: "h", Value: "v"}}, Value: []byte("x")},
		CoreMsg{SubID: 0x80000002, Subject: "a.b", Value: []byte{}},
	}
}

func TestPrimitivesRoundTrip(t *testing.T) {
	w := NewWriter(4) // forces growth
	w.U8(255)
	w.U16(65535)
	w.U32(0xffffffff)
	w.U64(1<<64 - 1)
	w.U64(0)
	w.Str("héllo")
	w.LStr("SELECT 'ü'")
	w.Bytes([]byte("raw"))
	w.OptStr(nil)
	w.OptStr(sp("x"))
	w.Headers([]Header{{"k", "v"}, {"k", "v2"}})
	buf, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(buf)
	must := func(v any, err error) any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := must(r.U8()); v != byte(255) {
		t.Fatal(v)
	}
	if v := must(r.U16()); v != uint16(65535) {
		t.Fatal(v)
	}
	if v := must(r.U32()); v != uint32(0xffffffff) {
		t.Fatal(v)
	}
	if v := must(r.U64()); v != uint64(1<<64-1) {
		t.Fatal(v)
	}
	if v := must(r.U64()); v != uint64(0) {
		t.Fatal(v)
	}
	if v := must(r.Str()); v != "héllo" {
		t.Fatal(v)
	}
	if v := must(r.LStr()); v != "SELECT 'ü'" {
		t.Fatal(v)
	}
	if v := must(r.Bytes()); string(v.([]byte)) != "raw" {
		t.Fatal(v)
	}
	if v := must(r.OptStr()); v.(*string) != nil {
		t.Fatal(v)
	}
	if v := must(r.OptStr()); *v.(*string) != "x" {
		t.Fatal(v)
	}
	h := must(r.Headers()).([]Header)
	if !reflect.DeepEqual(h, []Header{{"k", "v"}, {"k", "v2"}}) {
		t.Fatal(h)
	}
	if err := r.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestPrimitiveEncodings(t *testing.T) {
	w := NewWriter(0)
	w.Str("é")
	if b, _ := w.Finish(); !bytes.Equal(b, unhex(t, "0200 c3a9")) {
		t.Fatalf("%x", b)
	}
	w = NewWriter(0)
	w.U64(0x0102030405060708)
	if b, _ := w.Finish(); !bytes.Equal(b, unhex(t, "0807060504030201")) {
		t.Fatalf("%x", b)
	}
}

func TestReaderRejectsMalformedInput(t *testing.T) {
	cases := []struct {
		name string
		f    func() error
		want string
	}{
		{"short u16", func() error { _, err := NewReader(unhex(t, "01")).U16(); return err }, "truncated"},
		{"short str", func() error { _, err := NewReader(unhex(t, "0500 6162")).Str(); return err }, "truncated"},
		{"trailing", func() error { return NewReader(unhex(t, "00")).Finish() }, "trailing"},
		{"opt flag", func() error { _, err := NewReader(unhex(t, "02")).OptStr(); return err }, "option flag"},
		{"utf8", func() error { _, err := NewReader(unhex(t, "0200 c328")).Str(); return err }, "UTF-8"},
		{"headers", func() error { _, err := NewReader(unhex(t, "ffff")).Headers(); return err }, "header count"},
	}
	for _, c := range cases {
		err := c.f()
		var de *DecodeError
		if !errors.As(err, &de) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want DecodeError containing %q", c.name, err, c.want)
		}
	}
}

func TestWriterRejectsOversizeValues(t *testing.T) {
	w := NewWriter(0)
	w.Str(strings.Repeat("x", 70_000))
	var ee *EncodeError
	if _, err := w.Finish(); !errors.As(err, &ee) {
		t.Fatalf("got %v", err)
	}
	if _, err := EncodeFrameOf(Publish{Stream: strings.Repeat("s", 70_000)}, 1); !errors.As(err, &ee) {
		t.Fatalf("got %v", err)
	}
}

func TestFrameHeader(t *testing.T) {
	f, err := EncodeFrame(0x01, 7, unhex(t, "aabb"))
	if err != nil || !bytes.Equal(f, unhex(t, "02 01 07000000 02000000 aabb")) {
		t.Fatalf("%x %v", f, err)
	}
	full, err := EncodeFrameOf(Connect{ClientID: "c", Token: sp("t")}, 1)
	if err != nil || !bytes.Equal(full, unhex(t, "02 01 01000000 07000000 | 0100 63 01 0100 74")) {
		t.Fatalf("%x %v", full, err)
	}
}

// oneByteReader returns one byte per Read call.
type oneByteReader struct{ b []byte }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, errEOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.b[0]
	r.b = r.b[1:]
	return 1, nil
}

var errEOF = fmt.Errorf("EOF")

func TestReadFrameSplitAtEveryByte(t *testing.T) {
	var stream []byte
	for _, x := range []struct {
		m    Message
		corr uint32
	}{{Pong{}, 1}, {PublishOk{Offset: 3}, 2}, {Deliver{SubID: 9, Records: []WireRecord{rec(2)}}, 0}} {
		f, err := EncodeFrameOf(x.m, x.corr)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, f...)
	}
	r := &oneByteReader{b: stream}
	var got [][2]uint32
	for i := 0; i < 3; i++ {
		f, err := ReadFrame(r)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]uint32{uint32(f.Opcode), f.CorrelationID})
	}
	want := [][2]uint32{{uint32(OpPong), 1}, {uint32(OpPublishOk), 2}, {uint32(OpDeliver), 0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestReadFrameRejectsBadVersionAndOversize(t *testing.T) {
	if _, err := ReadFrame(bytes.NewReader(unhex(t, "01 80 00000000 00000000"))); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("got %v", err)
	}
	big := unhex(t, "02 80 00000000 01000001") // 16 MiB + 1
	if _, err := ReadFrame(bytes.NewReader(big)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("got %v", err)
	}
	if _, err := EncodeFrame(0x10, 1, make([]byte, MaxPayload+1)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("got %v", err)
	}
}

func TestEveryRequestRoundTrips(t *testing.T) {
	for _, req := range allRequests() {
		frame, err := EncodeFrameOf(req, 42)
		if err != nil {
			t.Fatalf("%T: %v", req, err)
		}
		f, err := ReadFrame(bytes.NewReader(frame))
		if err != nil {
			t.Fatal(err)
		}
		if f.Opcode != req.Opcode() || f.CorrelationID != 42 {
			t.Fatalf("%T: header %v", req, f)
		}
		got, err := DecodeRequest(f.Opcode, f.Payload)
		if err != nil {
			t.Fatalf("%T: %v", req, err)
		}
		again, err := Encode(got)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, f.Payload) || reflect.TypeOf(got) != reflect.TypeOf(req) {
			t.Fatalf("%T did not round-trip: %+v", req, got)
		}
	}
}

func TestEveryResponseRoundTrips(t *testing.T) {
	for _, resp := range allResponses() {
		frame, err := EncodeFrameOf(resp, 7)
		if err != nil {
			t.Fatalf("%T: %v", resp, err)
		}
		f, err := ReadFrame(bytes.NewReader(frame))
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeResponse(f.Opcode, f.Payload)
		if err != nil {
			t.Fatalf("%T: %v", resp, err)
		}
		again, _ := Encode(got)
		if !bytes.Equal(again, f.Payload) || reflect.TypeOf(got) != reflect.TypeOf(resp) {
			t.Fatalf("%T did not round-trip: %+v", resp, got)
		}
	}
}

func TestDecodeRejectsTruncatedTrailingAndHostile(t *testing.T) {
	if _, err := DecodeRequest(OpPing, unhex(t, "09")); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("got %v", err)
	}
	sub, _ := Encode(Subscribe{Consumer: "c", Credits: 1})
	if _, err := DecodeRequest(OpSubscribe, sub[:len(sub)-1]); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("got %v", err)
	}
	w := NewWriter(0)
	w.Str("s")
	w.U32(0xffffffff)
	b, _ := w.Finish()
	if _, err := DecodeRequest(OpPublishBatch, b); err == nil || !strings.Contains(err.Error(), "exceeds payload") {
		t.Fatalf("got %v", err)
	}
	w = NewWriter(0)
	w.U64(0)
	w.U64(0)
	w.U32(0xffffffff)
	b, _ = w.Finish()
	if _, err := DecodeResponse(OpReadResult, b); err == nil || !strings.Contains(err.Error(), "exceeds payload") {
		t.Fatalf("got %v", err)
	}
	if _, err := DecodeResponse(OpPublish, nil); err == nil || !strings.Contains(err.Error(), "not a server response") {
		t.Fatalf("got %v", err)
	}
}

// Request fixtures derived from `Writer` in crates/exspeed-protocol/src/client.rs
// (the same bytes as sdks/typescript/test/unit/protocol.test.ts).
func TestRequestFixturesMatchRust(t *testing.T) {
	fixtures := []struct {
		name string
		req  Message
		hex  string
	}{
		{"Connect", Connect{ClientID: "c", Token: sp("t")}, "0100 63 | 01 0100 74"},
		{"Connect without token", Connect{ClientID: "c"}, "0100 63 | 00"},
		{"Ping", Ping{}, ""},
		{"Publish", Publish{Stream: "s", Record: pr}, `0100 73
		 0300 612e62
		 01 01000000 6b
		 07000000 7b2278223a317d
		 0100 0200 6831 0200 7631
		 01 0300 6d2d31`},
		{"PublishBatch", PublishBatch{Stream: "s", Records: []PublishRecord{pr, emptyPr}}, `0100 73 | 02000000
		 0300 612e62 01 01000000 6b 07000000 7b2278223a317d 0100 0200 6831 0200 7631 01 0300 6d2d31
		 0000 00 00000000 0000 00`},
		{"CreateStream", CreateStream{Spec: spec},
			"0100 73 0100000000000000 0200000000000000 0300000000000000 0400000000000000 01"},
		{"Query", Query{SQL: "SELECT 1"}, "08000000 53454c4543542031"},
		{"ListConsumers all", ListConsumers{}, "00"},
		{"ListConsumers stream", ListConsumers{Stream: sp("s")}, "01 0100 73"},
		{"Seek time", SeekConsumer{Consumer: "c", Kind: SeekTime, Value: 9}, "0100 63 03 0900000000000000"},
		{"Seek latest", SeekConsumer{Consumer: "c", Kind: SeekLatest}, "0100 63 01 0000000000000000"},
		{"Subscribe", Subscribe{Consumer: "c", Credits: 100}, "0100 63 64000000"},
		{"Credit", Credit{SubID: 3, Credits: 10}, "03000000 0a000000"},
		{"Unsubscribe", Unsubscribe{SubID: 3}, "03000000"},
		{"Pull", Pull{Consumer: "c", MaxMessages: 10, MaxBytes: 1024, ExpiresMs: 500}, "0100 63 0a000000 00040000 f4010000"},
		{"Ack", Ack{Consumer: "c", Offsets: []uint64{1, 2, 3}},
			"0100 63 03000000 0100000000000000 0200000000000000 0300000000000000"},
		{"Nack", Nack{Consumer: "c", Offset: 4, DelayMs: 100}, "0100 63 0400000000000000 64000000"},
		{"Term", Term{Consumer: "c", Offset: 5, Reason: "bad"}, "0100 63 0500000000000000 0300 626164"},
		{"Read", Read{Stream: "s", From: 7, MaxRecords: 100, MaxBytes: 1 << 20, WaitMs: 1000, Filter: "a.*"},
			"0100 73 0700000000000000 64000000 00001000 e8030000 0300 612e2a"},
		// The fixtures below were generated with the Rust encoder (Request::into_frame).
		{"CreateStream with limits", CreateStream{Spec: limitedSpec},
			"0100 71 0000000000000000 0000000000000000 0000000000000000 0000000000000000 00 8d000000 " +
				hex.EncodeToString([]byte(limitsJSON))},
		{"CorePublish", CorePublish{Subject: "a.b", ReplyTo: sp("r"), Headers: []Header{{"h", "v"}}, Value: []byte("x")},
			"0300 612e62 | 01 0100 72 | 0100 0100 68 0100 76 | 01000000 78"},
		{"CorePublish bare", CorePublish{Subject: "a", Value: []byte{}}, "0100 61 | 00 | 0000 | 00000000"},
		{"CoreSubscribe", CoreSubscribe{Subject: "a.*", Queue: sp("q")}, "0300 612e2a 01 0100 71"},
		{"CoreSubscribe bare", CoreSubscribe{Subject: "a.*"}, "0300 612e2a 00"},
		{"KvCreateBucket", KvCreateBucket{Bucket: "b", History: 5, TTLMs: 1000},
			"0100 62 0500000000000000 e803000000000000 0000000000000000"},
		{"KvPut expecting revision 0", KvPut{Bucket: "b", Key: "k", Value: []byte("v"), ExpectedRevision: u64p(0)},
			"0100 62 0100 6b 01000000 76 01 0000000000000000 00"},
		{"KvPut with TTL", KvPut{Bucket: "b", Key: "k", Value: []byte("v"), TTLMs: u64p(500)},
			"0100 62 0100 6b 01000000 76 00 01 f401000000000000"},
		{"KvGet at revision", KvGet{Bucket: "b", Key: "k", Revision: u64p(3)}, "0100 62 0100 6b 01 0300000000000000"},
		{"KvGet", KvGet{Bucket: "b", Key: "k"}, "0100 62 0100 6b 00"},
		{"KvDelete purge", KvDelete{Bucket: "b", Key: "k", Purge: true, ExpectedRevision: u64p(7)},
			"0100 62 0100 6b 01 01 0700000000000000"},
		{"KvDelete", KvDelete{Bucket: "b", Key: "k"}, "0100 62 0100 6b 00 00"},
		{"KvKeys", KvKeys{Bucket: "b", Filter: "a.*"}, "0100 62 0300 612e2a"},
		{"KvHistory", KvHistory{Bucket: "b", Key: "k"}, "0100 62 0100 6b"},
	}
	for _, f := range fixtures {
		got, err := Encode(f.req)
		if err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if want := unhex(t, f.hex); !bytes.Equal(got, want) {
			t.Errorf("%s:\n got %x\nwant %x", f.name, got, want)
		}
	}
}

func TestConsumerSpecPayloadIsBytesOfJSON(t *testing.T) {
	payload, _ := Encode(CreateConsumer{Spec: []byte(consumerJSON)})
	if int(payload[0])|int(payload[1])<<8 != len(consumerJSON) || string(payload[4:]) != consumerJSON {
		t.Fatalf("%q", payload)
	}
}

func TestStreamSpecWithoutLimitsDecodesWithoutTrailer(t *testing.T) {
	payload := unhex(t, "0100 73 0100000000000000 0000000000000000 0000000000000000 0000000000000000 00")
	m, err := DecodeRequest(OpCreateStream, payload)
	if err != nil {
		t.Fatal(err)
	}
	want := CreateStream{Spec: StreamSpec{Name: "s", MaxAgeSecs: 1}}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("%+v", m)
	}
}

// Response fixtures derived from `Writer` in crates/exspeed-protocol/src/client.rs.
func TestResponseFixturesMatchRust(t *testing.T) {
	fixtures := []struct {
		name string
		resp Message
		hex  string
	}{
		{"Error", Error{Code: 404, Message: "nope"}, "9401 0400 6e6f7065 00"},
		{"Error with detail", Error{Code: 503, Message: "not leader", Detail: []byte(`{"leader":"h:1"}`)},
			"f701 0a00 6e6f74206c6561646572 01 10000000 7b226c6561646572223a22683a31227d"},
		{"ConnectOk", ConnectOk{ServerVersion: "0.6.0", NodeID: "n1", Leader: sp("h:5933")},
			"0500 302e362e30 0200 6e31 01 0600 683a35393333"},
		{"PublishOk", PublishOk{Offset: 9, Duplicate: true}, "0900000000000000 01"},
		{"PublishBatchOk", PublishBatchOk{Results: []PublishResult{{Offset: 1}, {Offset: 1, Duplicate: true}}},
			"02000000 0100000000000000 00 0100000000000000 01"},
		{"SubscribeOk", SubscribeOk{SubID: 2}, "02000000"},
		{"SubscriptionEnded", SubscriptionEnded{SubID: 2, Code: 404, Message: "consumer deleted"},
			"02000000 9401 1000 636f6e73756d65722064656c65746564"},
		{"Deliver", Deliver{SubID: 2, Records: []WireRecord{rec(2)}}, `02000000 | 01000000
		 36000000 06760cef 0200
		 0200000000000000 02002a36fe9c9717
		 0800 6f72646572732e32
		 01 02000000 6b32
		 02000000 0202
		 0100 0100 68 0200 7632`},
		{"Messages", Messages{Records: []WireRecord{rec(0)}}, `01000000
		 34000000 b6f9c422 0000
		 0000000000000000 00002a36fe9c9717
		 0800 6f72646572732e30
		 01 02000000 6b30
		 00000000
		 0100 0100 68 0200 7630`},
		{"ReadResult", ReadResult{NextOffset: 10, HighWatermark: 12, Records: []WireRecord{rec(1)}},
			`0a00000000000000 0c00000000000000 01000000
		 2f000000 f194cd5a 0100
		 0100000000000000 01002a36fe9c9717
		 0800 6f72646572732e31
		 00
		 01000000 01
		 0100 0100 68 0200 7631`},
		{"CoreMsg", CoreMsg{SubID: 0x80000001, Subject: "a", ReplyTo: sp("r"), Headers: []Header{{"h", "v"}}, Value: []byte("x")},
			"01000080 0100 61 01 0100 72 0100 0100 68 0100 76 01000000 78"},
	}
	for _, f := range fixtures {
		want := unhex(t, f.hex)
		got, err := Encode(f.resp)
		if err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s encode:\n got %x\nwant %x", f.name, got, want)
		}
		dec, err := DecodeResponse(f.resp.Opcode(), want)
		if err != nil {
			t.Fatalf("%s decode: %v", f.name, err)
		}
		if again := mustEncode(t, dec); !bytes.Equal(again, want) || reflect.TypeOf(dec) != reflect.TypeOf(f.resp) {
			t.Errorf("%s decode: got %+v", f.name, dec)
		}
	}
}

func TestDecodedRecordFields(t *testing.T) {
	m, err := DecodeResponse(OpMessages, unhex(t, `01000000
		 36000000 06760cef 0200
		 0200000000000000 02002a36fe9c9717
		 0800 6f72646572732e32
		 01 02000000 6b32
		 02000000 0202
		 0100 0100 68 0200 7632`))
	if err != nil {
		t.Fatal(err)
	}
	r := m.(Messages).Records[0]
	if r.Offset != 2 || r.TimestampNs != 1_700_000_000_000_000_002 || r.DeliveryCount != 2 ||
		r.Subject != "orders.2" || string(r.Key) != "k2" || !bytes.Equal(r.Value, []byte{2, 2}) ||
		!reflect.DeepEqual(r.Headers, []Header{{"h", "v2"}}) {
		t.Fatalf("%+v", r)
	}
	// A record without a key decodes with a nil key.
	m, _ = DecodeResponse(OpMessages, mustEncode(t, Messages{Records: []WireRecord{rec(1)}}))
	if m.(Messages).Records[0].Key != nil {
		t.Fatal("key should be nil")
	}
}

func mustEncode(t *testing.T, m Message) []byte {
	t.Helper()
	b, err := Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRecordCRCIgnoresDeliveryCount(t *testing.T) {
	r := rec(2)
	enc, err := EncodeRecord(&r)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != 0x36+4 || !VerifyRecordCRC(enc) {
		t.Fatalf("len %d", len(enc))
	}
	enc[8], enc[9] = 7, 0 // the server patches delivery_count in place
	if !VerifyRecordCRC(enc) {
		t.Fatal("CRC must not cover delivery_count")
	}
	payload := append(unhex(t, "01000000"), enc...)
	m, err := DecodeResponse(OpMessages, payload)
	if err != nil || m.(Messages).Records[0].DeliveryCount != 7 {
		t.Fatalf("%v %v", m, err)
	}
	enc[len(enc)-1] ^= 1
	if VerifyRecordCRC(enc) {
		t.Fatal("corruption not detected")
	}
	payload = append(unhex(t, "01000000"), enc...)
	if _, err := DecodeResponse(OpMessages, payload); err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("got %v", err)
	}
}

func TestCRC32CCheckValue(t *testing.T) {
	if got := CRC32C([]byte("123456789")); got != 0xe3069283 {
		t.Fatalf("%x", got)
	}
}

func TestRecordLengthMustMatchContents(t *testing.T) {
	r := rec(1)
	enc, _ := EncodeRecord(&r)
	enc[0]++ // len one larger than the fields
	enc = append(enc, 0)
	payload := append(unhex(t, "01000000"), enc...)
	var de *DecodeError
	if _, err := DecodeResponse(OpMessages, payload); !errors.As(err, &de) {
		t.Fatalf("got %v", err)
	}
}
