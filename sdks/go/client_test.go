package exspeed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

func TestHandshakeSendsClientIDAndToken(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s, WithToken("secret"), WithClientID("unit"))
	r := s.last().received()[0]
	eq(t, r.req, proto.Message(proto.Connect{ClientID: "unit", Token: sp("secret")}))
	if r.corr == 0 {
		t.Fatal("handshake must use a non-zero correlation id")
	}
	eq(t, c.ServerInfo(), ServerInfo{ServerVersion: "test", NodeID: "n1"})
	if !c.Connected() {
		t.Fatal("not connected")
	}
}

func TestHandshakeRejectedWith401(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, ok := req.(proto.Connect); ok {
			fc.reply(corr, proto.Error{Code: 401, Message: "unauthorized"})
			fc.drop()
			return true
		}
		return false
	})
	_, err := Connect(testCtx(t), s.addr(), WithToken("bad"))
	mustCode(t, err, 401)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatal("errors.Is(err, ErrUnauthorized) should hold")
	}
}

func TestConnectFailsWithConnectionErrorWhenNothingListens(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	addr := s.addr()
	s.close()
	_, err := Connect(testCtx(t), addr)
	var ce *ConnectionError
	if !errors.As(err, &ce) {
		t.Fatalf("got %v", err)
	}
}

func TestDefaultPortIsAddedToBareHosts(t *testing.T) {
	eq(t, normalizeAddr("example.com"), "example.com:5933")
	eq(t, normalizeAddr("10.0.0.1:7000"), "10.0.0.1:7000")
	eq(t, normalizeAddr("[::1]"), "[::1]:5933")
}

func TestOutOfOrderResponsesMatchByCorrelationID(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(*fakeConn, uint32, proto.Message) bool { return true })
	ctx := testCtx(t)
	type res struct {
		r   PublishResult
		err error
	}
	a, b := make(chan res, 1), make(chan res, 1)
	go func() {
		r, err := c.Publish(ctx, "s", PublishRecord{Subject: "a", Value: []byte("1")})
		a <- res{r, err}
	}()
	s.until(func() bool { return len(s.last().of(proto.Publish{})) == 1 })
	go func() {
		r, err := c.Publish(ctx, "s", PublishRecord{Subject: "b", Value: []byte("2")})
		b <- res{r, err}
	}()
	s.until(func() bool { return len(s.last().of(proto.Publish{})) == 2 })
	pubs := s.last().of(proto.Publish{})
	s.last().reply(pubs[1].corr, proto.PublishOk{Offset: 2})
	s.last().reply(pubs[0].corr, proto.PublishOk{Offset: 1, Duplicate: true})
	ra, rb := <-a, <-b
	if ra.err != nil || rb.err != nil {
		t.Fatal(ra.err, rb.err)
	}
	eq(t, ra.r, PublishResult{Offset: 1, Duplicate: true})
	eq(t, rb.r, PublishResult{Offset: 2})
}

func TestPublishEncodesRecordsAndOptions(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if b, ok := req.(proto.PublishBatch); ok {
			res := make([]proto.PublishResult, len(b.Records))
			for i := range res {
				res[i].Offset = uint64(i)
			}
			fc.reply(corr, proto.PublishBatchOk{Results: res})
			return true
		}
		return false
	})
	at := time.UnixMilli(1_700_000_000_000)
	got, err := c.PublishBatch(testCtx(t), "s", []PublishRecord{
		{Subject: "a", Value: []byte{1, 2}},
		{Subject: "a", Value: []byte(`{"id":1}`), Key: []byte("k"), Headers: Headers("x", "y"), MsgID: "m1"},
		// PublishRecord::new("a", "v").ttl(500ms).delay(2s).deliver_at(1700000000000).priority(7) in Rust
		{Subject: "a", Value: []byte("v"), Headers: Headers("trace-id", "t"), TTL: 500 * time.Millisecond,
			Delay: 2 * time.Second, DeliverAt: at, Priority: 7},
		{Subject: "a", TTL: 200 * time.Microsecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(got), 4)
	recs := s.last().of(proto.PublishBatch{})[0].req.(proto.PublishBatch).Records
	eq(t, recs[0].Value, []byte{1, 2})
	eq(t, recs[0].Key, []byte(nil))
	eq(t, recs[0].MsgID, (*string)(nil))
	eq(t, string(recs[1].Key), "k")
	eq(t, recs[1].Headers, []proto.Header{{Key: "x", Value: "y"}})
	eq(t, *recs[1].MsgID, "m1")
	eq(t, recs[2].Headers, []proto.Header{
		{Key: "trace-id", Value: "t"},
		{Key: "exspeed-ttl", Value: "500ms"},
		{Key: "exspeed-delay", Value: "2000ms"},
		{Key: "exspeed-deliver-at", Value: "1700000000000"},
		{Key: "exspeed-priority", Value: "7"},
	})
	eq(t, recs[3].Headers, []proto.Header{{Key: "exspeed-ttl", Value: "1ms"}})
	eq(t, recs[3].Value, []byte{})
}

func TestPublishRejectsInvalidOptions(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	ctx := testCtx(t)
	for _, r := range []PublishRecord{
		{Subject: "a", Priority: 10},
		{Subject: "a", Priority: -1},
		{Subject: "a", TTL: -time.Second},
		{Subject: "a", Delay: -time.Second},
		{Subject: strings.Repeat("x", 70_000)},
	} {
		if _, err := c.Publish(ctx, "s", r); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%+v: got %v", r.Priority, err)
		}
	}
	if len(s.last().of(proto.Publish{})) != 0 {
		t.Fatal("nothing should have been sent")
	}
}

func TestServerErrorsCarryCodeDetailAndLeaderHint(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch r := req.(type) {
		case proto.StreamInfo:
			fc.reply(corr, proto.Error{Code: 503, Message: "not the leader", Detail: []byte(`{"leader":"h2:5933"}`)})
		case proto.Publish:
			if r.Record.Subject == "dup" {
				fc.reply(corr, proto.Error{Code: 409, Message: "msg_id reused", Detail: []byte(`{"stored_offset":7}`)})
			} else {
				fc.reply(corr, proto.Error{Code: 429, Message: "full", Detail: []byte(`{"retry_after_secs":30}`)})
			}
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	_, err := c.StreamInfo(ctx, "s")
	se := mustCode(t, err, 503)
	eq(t, se.Message, "not the leader")
	eq(t, string(se.Detail), `{"leader":"h2:5933"}`)
	eq(t, se.LeaderHint(), "h2:5933")
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrNotFound) {
		t.Fatal("errors.Is should match the code only")
	}
	var detail struct{ Leader string }
	if err := se.DecodeDetail(&detail); err != nil || detail.Leader != "h2:5933" {
		t.Fatal(detail, err)
	}

	_, err = c.Publish(ctx, "s", PublishRecord{Subject: "dup"})
	if off, ok := mustCode(t, err, 409).StoredOffset(); !ok || off != 7 {
		t.Fatal(off, ok)
	}
	_, err = c.Publish(ctx, "s", PublishRecord{Subject: "x"})
	if d, ok := mustCode(t, err, 429).RetryAfter(); !ok || d != 30*time.Second {
		t.Fatal(d, ok)
	}
}

func TestRequestsTimeOut(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s, WithRequestTimeout(100*time.Millisecond))
	s.setHandler(func(*fakeConn, uint32, proto.Message) bool { return true })
	_, err := c.Metadata(testCtx(t))
	var te *TimeoutError
	if !errors.As(err, &te) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

func TestContextCancellationAbandonsTheRequest(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		_, isMeta := req.(proto.Metadata)
		return isMeta
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Metadata(ctx)
		done <- err
	}()
	s.until(func() bool { return len(s.last().of(proto.Metadata{})) == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	// A late answer is dropped without trouble.
	s.last().reply(s.last().of(proto.Metadata{})[0].corr, proto.JSON{Data: []byte(`{}`)})
	if _, err := c.Ping(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPullAndReadGetExtraTimeForTheirWait(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s, WithRequestTimeout(50*time.Millisecond))
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Pull:
			time.AfterFunc(150*time.Millisecond, func() { fc.reply(corr, proto.Messages{Records: []proto.WireRecord{wrec(3, "x")}}) })
		case proto.Read:
			time.AfterFunc(150*time.Millisecond, func() { fc.reply(corr, proto.ReadResult{NextOffset: 1, HighWatermark: 1}) })
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	msgs, err := c.Pull(ctx, "c", PullOptions{Expires: 200 * time.Millisecond})
	if err != nil || len(msgs) != 1 || msgs[0].Offset != 3 {
		t.Fatal(msgs, err)
	}
	eq(t, s.last().of(proto.Pull{})[0].req, proto.Message(proto.Pull{Consumer: "c", MaxMessages: 100, ExpiresMs: 200}))
	r, err := c.Read(ctx, "s", ReadOptions{Wait: 200 * time.Millisecond, Filter: "a.*"})
	if err != nil || r.NextOffset != 1 {
		t.Fatal(r, err)
	}
	eq(t, s.last().of(proto.Read{})[0].req, proto.Message(proto.Read{Stream: "s", MaxRecords: 100, WaitMs: 200, Filter: "a.*"}))
}

func TestJSONRepliesDecode(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Metadata:
			fc.reply(corr, proto.JSON{Data: []byte(`{"node_id":"n1","is_leader":true,"leader":null,"server_version":"x"}`)})
		case proto.StreamInfo:
			fc.reply(corr, proto.JSON{Data: []byte(`{"name":"s","earliest_offset":2,"next_offset":5,"records":3,` +
				`"config":{"max_age_secs":60,"max_bytes":0,"dedup_window_secs":120,"dedup_max_entries":10,"compaction":false,` +
				`"max_msgs":2,"discard":"new","allow_msg_ttl":true,"retention":"limits"},"internal":false}`)})
		case proto.ListStreams:
			fc.reply(corr, proto.JSON{Data: []byte(`[{"name":"a","config":{}},{"name":"b","config":{}}]`)})
		case proto.Query:
			fc.reply(corr, proto.JSON{Data: []byte(`{"columns":["n"],"rows":[[3]],"row_count":1,"execution_time_ms":2,"truncated":false}`)})
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	m, err := c.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, *m, Metadata{NodeID: "n1", IsLeader: true, ServerVersion: "x"})
	info, err := c.StreamInfo(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "s" || info.EarliestOffset != 2 || info.NextOffset != 5 || info.Records != 3 ||
		info.Config.MaxAgeSecs != 60 || info.Config.MaxMsgs != 2 || info.Config.Discard != DiscardNew ||
		!info.Config.AllowMsgTTL || info.Config.Retention != RetentionLimits || len(info.Raw) == 0 {
		t.Fatalf("%+v", info)
	}
	list, err := c.ListStreams(ctx)
	if err != nil || len(list) != 2 || list[1].Name != "b" {
		t.Fatal(list, err)
	}
	q, err := c.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, q.Columns, []string{"n"})
	eq(t, q.Rows, [][]any{{json.Number("3")}})
	eq(t, q.RowCount, 1)
	eq(t, s.last().of(proto.Query{})[0].req, proto.Message(proto.Query{SQL: "SELECT 1"}))
}

func TestPendingRequestsFailWhenTheConnectionDrops(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(*fakeConn, uint32, proto.Message) bool { return true })
	done := make(chan error, 1)
	go func() {
		_, err := c.Metadata(context.Background())
		done <- err
	}()
	s.until(func() bool { return len(s.last().of(proto.Metadata{})) == 1 })
	s.last().drop()
	var ce *ConnectionError
	if err := <-done; !errors.As(err, &ce) {
		t.Fatalf("got %v", err)
	}
	waitFor(t, func() bool { return !c.Connected() })
	if _, err := c.Ping(testCtx(t)); !errors.As(err, &ce) {
		t.Fatalf("got %v", err)
	}
}

func TestCallsAfterCloseFailWithErrClosed(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ping(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("waitFor: timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- wire mappings ----

// The serde_json encoding of the consumer spec in the Rust round-trip test.
const rustConsumerJSON = `{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},` +
	`"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],` +
	`"max_ack_pending":1000,"dlq_stream":"s-dlq","ephemeral":false,` +
	`"dead_letter_expired":false,"header_match":"all","single_active":false,"priority_window":0}`

func TestConsumerSpecJSONMatchesSerde(t *testing.T) {
	// Every field set (false and zero included): the same bytes as serde_json.
	w := wireConsumerSpec{
		Name: "c", Stream: "s", FilterSubjects: []string{"orders.>"},
		Deliver: DeliverFromTime(time.UnixMilli(123)).wire(), Ack: ptr("explicit"),
		AckWaitMs: ptr(uint64(30000)), MaxDeliver: ptr(uint32(5)), BackoffMs: []uint64{100, 1000},
		MaxAckPending: ptr(uint32(1000)), DLQStream: ptr("s-dlq"), Ephemeral: ptr(false),
		DeadLetterExpired: ptr(false), HeaderMatch: ptr("all"), SingleActive: ptr(false), PriorityWindow: ptr(uint32(0)),
	}
	b, err := marshalJSON(w)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(b), rustConsumerJSON)
	payload, _ := proto.Encode(proto.CreateConsumer{Spec: b})
	eq(t, string(payload[4:]), rustConsumerJSON)
}

func TestConsumerSpecMapping(t *testing.T) {
	full := ConsumerSpec{
		Name: "c", Stream: "s", FilterSubjects: []string{"orders.>"}, Deliver: DeliverFromTime(time.UnixMilli(123)),
		Ack: AckExplicit, AckWait: 30 * time.Second, MaxDeliver: 5, Backoff: []time.Duration{100 * time.Millisecond, time.Second},
		MaxAckPending: 1000, DLQStream: "s-dlq", DeadLetterExpired: true, FilterHeaders: map[string]string{"tenant": "acme"},
		HeaderMatch: HeaderMatchAny, SingleActive: true, PriorityWindow: 50,
	}
	w, err := full.toWire()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := marshalJSON(w)
	eq(t, string(raw), `{"name":"c","stream":"s","filter_subjects":["orders.>"],"deliver":{"from_time":123},`+
		`"ack":"explicit","ack_wait_ms":30000,"max_deliver":5,"backoff_ms":[100,1000],"max_ack_pending":1000,`+
		`"dlq_stream":"s-dlq","dead_letter_expired":true,"filter_headers":{"tenant":"acme"},"header_match":"any",`+
		`"single_active":true,"priority_window":50}`)
	// json.Marshal of a ConsumerSpec gives the same JSON (modulo HTML escaping).
	viaJSON, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var m1, m2 map[string]any
	_ = json.Unmarshal(raw, &m1)
	_ = json.Unmarshal(viaJSON, &m2)
	eq(t, m1, m2)

	// Unset fields are left out, so the server applies its defaults.
	minimal, _ := (&ConsumerSpec{Name: "c", Stream: "s"}).toWire()
	raw, _ = marshalJSON(minimal)
	eq(t, string(raw), `{"name":"c","stream":"s"}`)
	// -1 means unlimited (0 on the wire); deliver policies.
	unl, _ := (&ConsumerSpec{Name: "c", Stream: "s", MaxDeliver: -1, MaxAckPending: -1, Deliver: DeliverFromOffset(5), Ack: AckNone}).toWire()
	raw, _ = marshalJSON(unl)
	eq(t, string(raw), `{"name":"c","stream":"s","deliver":{"from_offset":5},"ack":"none","max_deliver":0,"max_ack_pending":0}`)
	for _, d := range []DeliverPolicy{DeliverAll(), DeliverNew()} {
		w, _ := (&ConsumerSpec{Name: "c", Stream: "s", Deliver: d}).toWire()
		raw, _ = marshalJSON(w)
		eq(t, string(raw), fmt.Sprintf(`{"name":"c","stream":"s","deliver":"%s"}`, d))
	}
	for _, bad := range []ConsumerSpec{{Stream: "s"}, {Name: "c"}, {Name: "c", Stream: "s", MaxDeliver: -2}} {
		if _, err := bad.toWire(); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%+v: %v", bad, err)
		}
	}

	// Decoding the server's (fully resolved) spec round-trips.
	var back ConsumerSpec
	if err := json.Unmarshal([]byte(`{"name":"c","stream":"s","filter_subjects":[],"deliver":{"from_offset":9},"ack":"explicit",`+
		`"ack_wait_ms":1500,"max_deliver":0,"backoff_ms":[10],"max_ack_pending":0,"dlq_stream":null,"ephemeral":true,`+
		`"dead_letter_expired":false,"header_match":"all","single_active":false,"priority_window":3}`), &back); err != nil {
		t.Fatal(err)
	}
	if off, ok := back.Deliver.FromOffset(); !ok || off != 9 {
		t.Fatal(back.Deliver)
	}
	if back.AckWait != 1500*time.Millisecond || back.MaxDeliver != -1 || back.MaxAckPending != -1 || back.DLQStream != "" ||
		!back.Ephemeral || back.PriorityWindow != 3 || back.Backoff[0] != 10*time.Millisecond || back.HeaderMatch != HeaderMatchAll {
		t.Fatalf("%+v", back)
	}
}

func TestStreamLimitsTrailer(t *testing.T) {
	// No trailer when every limit is at its default.
	plain, err := (&StreamSpec{Name: "s", MaxAge: time.Second, Discard: DiscardOld, Retention: RetentionLimits}).toWire()
	if err != nil {
		t.Fatal(err)
	}
	eq(t, plain.Limits, []byte(nil))
	payload, _ := proto.Encode(proto.CreateStream{Spec: plain})
	eq(t, fmt.Sprintf("%x", payload), "010073"+"0100000000000000"+strings.Repeat("0", 48)+"00")

	// The Rust `limited` spec: the same bytes as the Rust encoder.
	limited, _ := (&StreamSpec{Name: "q", MaxMsgs: 10, Discard: DiscardNew, MaxMsgsPerSubject: 1, AllowMsgTTL: true,
		MsgTTL: 5 * time.Second, AllowDelayed: true, Retention: RetentionWorkQueue}).toWire()
	eq(t, string(limited.Limits), `{"max_msgs":10,"discard":"new","max_msgs_per_subject":1,"allow_msg_ttl":true,`+
		`"msg_ttl_ms":5000,"allow_delayed":true,"retention":"work_queue"}`)
	one, _ := (&StreamSpec{Name: "s", AllowDelayed: true}).toWire()
	eq(t, string(one.Limits), `{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,`+
		`"msg_ttl_ms":0,"allow_delayed":true,"retention":"limits"}`)
	for _, s := range []StreamSpec{{MaxMsgs: 1}, {Discard: DiscardNew}, {MaxMsgsPerSubject: 2}, {AllowMsgTTL: true},
		{MsgTTL: time.Millisecond}, {Retention: RetentionInterest}} {
		s.Name = "s"
		w, _ := s.toWire()
		if w.Limits == nil {
			t.Fatalf("%+v should send limits", s)
		}
	}
	spec, _ := (&StreamSpec{Name: "s", MaxAge: 1500 * time.Millisecond, DedupWindow: time.Minute, MaxBytes: 2, DedupMaxEntries: 4, Compaction: true}).toWire()
	eq(t, spec, proto.StreamSpec{Name: "s", MaxAgeSecs: 2, MaxBytes: 2, DedupWindowSecs: 60, DedupMaxEntries: 4, Compaction: true})
	// capture_subjects is sent only when there are some (the JSON pinned by
	// the capture_subjects test in crates/exspeed-common/src/limits.rs).
	none, _ := (&StreamSpec{Name: "s", CaptureSubjects: []string{}}).toWire()
	eq(t, none.Limits, []byte(nil))
	capt, _ := (&StreamSpec{Name: "s", CaptureSubjects: []string{"orders.>"}}).toWire()
	eq(t, string(capt.Limits), `{"max_msgs":0,"discard":"old","max_msgs_per_subject":0,"allow_msg_ttl":false,`+
		`"msg_ttl_ms":0,"allow_delayed":false,"retention":"limits","capture_subjects":["orders.>"]}`)
	if _, err := (&StreamSpec{}).toWire(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestNewMsgIDIsATimeOrderedUUIDv7(t *testing.T) {
	a := NewMsgID()
	time.Sleep(2 * time.Millisecond)
	b := NewMsgID()
	if len(a) != 36 || a[14] != '7' || !strings.ContainsRune("89ab", rune(a[19])) || a >= b {
		t.Fatalf("%s %s", a, b)
	}
}

// ---- keepalive and clusters ----

func TestKeepalivePings(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	connectFake(t, s, WithKeepalive(30*time.Millisecond))
	s.until(func() bool { return len(s.last().of(proto.Ping{})) >= 2 })
}

func TestClusterSeedsFollowTheLeaderHint(t *testing.T) {
	checkLeaks(t)
	leader := startFake(t)
	follower := startFake(t)
	meta := func(fc *fakeConn, corr uint32, isLeader bool, hint string) {
		var l any
		if hint != "" {
			l = hint
		}
		b, _ := json.Marshal(map[string]any{"node_id": "x", "is_leader": isLeader, "leader": l, "server_version": "t"})
		fc.reply(corr, proto.JSON{Data: b})
	}
	follower.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Connect:
			fc.reply(corr, proto.ConnectOk{ServerVersion: "t", NodeID: "f", Leader: sp(leader.addr())})
		case proto.Metadata:
			meta(fc, corr, false, leader.addr())
		default:
			return false
		}
		return true
	})
	leader.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Connect:
			fc.reply(corr, proto.ConnectOk{ServerVersion: "t", NodeID: "l"})
		case proto.Metadata:
			meta(fc, corr, true, "")
		default:
			return false
		}
		return true
	})
	c, err := Connect(testCtx(t), "", WithServers(follower.addr()), WithKeepalive(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	eq(t, c.ServerInfo().NodeID, "l")

	// Without seeds, a handshake naming another leader is followed too.
	c2, err := Connect(testCtx(t), follower.addr(), WithKeepalive(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	eq(t, c2.ServerInfo().NodeID, "l")
}

func TestEventHandlersRunInOrder(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var mu sync.Mutex
	var events []string
	add := func(e string) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	c := connectFake(t, s,
		WithReconnect(ReconnectPolicy{InitialDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond}),
		WithDisconnectHandler(func(error) { add("disconnect") }),
		WithReconnectHandler(func(ServerInfo) { add("reconnect") }),
		WithCloseHandler(func(err error) { add(fmt.Sprintf("close:%v", err)) }),
	)
	s.last().drop()
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(events) == 2 })
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(events) == 3 })
	mu.Lock()
	eq(t, events, []string{"disconnect", "reconnect", "close:<nil>"})
	mu.Unlock()
}

func TestZeroRequestTimeoutMeansNoTimeout(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s, WithRequestTimeout(0))
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, isPing := req.(proto.Ping); isPing {
			time.AfterFunc(50*time.Millisecond, func() { fc.reply(corr, proto.Pong{}) })
			return true
		}
		return false
	})
	if _, err := c.Ping(testCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestLoadTLSConfig(t *testing.T) {
	if _, err := LoadTLSConfig("/nonexistent/ca.pem", "", ""); err == nil {
		t.Fatal("missing CA file must fail")
	}
	cfg, err := LoadTLSConfig("", "", "")
	if err != nil || cfg.RootCAs != nil || len(cfg.Certificates) != 0 {
		t.Fatal(cfg, err)
	}
}
