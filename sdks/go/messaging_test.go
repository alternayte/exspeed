package exspeed

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

func ok(fc *fakeConn, corr uint32) { fc.reply(corr, proto.Ok{}) }

func nextCore(t *testing.T, s *CoreSubscription, timeout time.Duration) (*CoreMessage, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Next(ctx)
}

// ---- core pub/sub ----

func TestPublishCoreWaitsForOk(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, isPub := req.(proto.CorePublish); isPub {
			ok(fc, corr)
			return true
		}
		return false
	})
	if err := c.PublishCore(testCtx(t), "orders.created", []byte(`{"id":1}`), Headers("trace-id", "t")...); err != nil {
		t.Fatal(err)
	}
	p := s.last().of(proto.CorePublish{})[0]
	if p.corr == 0 {
		t.Fatal("needs a correlation id")
	}
	eq(t, p.req, proto.Message(proto.CorePublish{Subject: "orders.created", Headers: []proto.Header{{Key: "trace-id", Value: "t"}}, Value: []byte(`{"id":1}`)}))
}

func TestCoreSubscribeWithQueueGroupRespondAndUnsubscribe(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.CoreSubscribe:
			fc.replyMany(
				frame{corr, proto.SubscribeOk{SubID: 0x80000001}},
				frame{0, proto.CoreMsg{SubID: 0x80000001, Subject: "svc.echo", ReplyTo: sp("_INBOX.x.1"), Headers: []proto.Header{{Key: "h", Value: "1"}}, Value: []byte(`{"q":1}`)}},
				frame{0, proto.CoreMsg{SubID: 0x80000099, Subject: "other", Value: []byte("ignored")}},
			)
		case proto.CorePublish, proto.Unsubscribe:
			ok(fc, corr)
		default:
			return false
		}
		return true
	})
	sub, err := c.QueueSubscribeCore(testCtx(t), "svc.*", "workers")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, s.last().of(proto.CoreSubscribe{})[0].req, proto.Message(proto.CoreSubscribe{Subject: "svc.*", Queue: sp("workers")}))
	eq(t, sub.ID(), uint32(0x80000001))
	eq(t, sub.Queue(), "workers")
	m, err := nextCore(t, sub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var body struct{ Q int }
	if err := m.JSON(&body); err != nil || body.Q != 1 {
		t.Fatal(body, err)
	}
	h, _ := m.Header("h")
	eq(t, []string{m.Subject, m.ReplyTo, h}, []string{"svc.echo", "_INBOX.x.1", "1"})
	if err := m.Respond(testCtx(t), []byte("pong")); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last().of(proto.CorePublish{})[0].req, proto.Message(proto.CorePublish{Subject: "_INBOX.x.1", Value: []byte("pong")}))
	if _, err := nextCore(t, sub, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the other subscription's message must not be routed here: %v", err)
	}
	if err := sub.Unsubscribe(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last().of(proto.Unsubscribe{})[0].req, proto.Message(proto.Unsubscribe{SubID: 0x80000001}))
	eq(t, sub.EndReason(), &SubscriptionEndedError{Code: 0, Message: "unsubscribed"})
}

func TestRespondNeedsAReplySubject(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	m := &CoreMessage{Subject: "a", c: c}
	if err := m.Respond(testCtx(t), []byte("no")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestCoreSubscriptionEndsWhenTheServerEndsIt(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, isSub := req.(proto.CoreSubscribe); !isSub {
			return false
		}
		fc.replyMany(
			frame{corr, proto.SubscribeOk{SubID: 0x80000002}},
			frame{0, proto.CoreMsg{SubID: 0x80000002, Subject: "a", Value: []byte("1")}},
			frame{0, proto.SubscriptionEnded{SubID: 0x80000002, Code: 503, Message: "leadership moved"}},
		)
		return true
	})
	sub, err := c.SubscribeCore(testCtx(t), "a")
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for m := range sub.Messages() {
		seen = append(seen, m.Text())
	}
	eq(t, seen, []string{"1"})
	eq(t, sub.EndReason(), &SubscriptionEndedError{Code: 503, Message: "leadership moved"})
}

// ---- request-reply ----

type inboxState struct {
	mu   sync.Mutex
	subs []string
}

func (st *inboxState) list() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]string(nil), st.subs...)
}

// inboxServer answers CoreSubscribe (ids from 0x80000010) and CorePublish;
// responses are sent by the test.
func inboxServer(s *fakeServer, publish handlerFunc) *inboxState {
	st := &inboxState{}
	id := uint32(0x80000010)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch r := req.(type) {
		case proto.CoreSubscribe:
			st.mu.Lock()
			st.subs = append(st.subs, r.Subject)
			sid := id
			id++
			st.mu.Unlock()
			fc.reply(corr, proto.SubscribeOk{SubID: sid})
		case proto.CorePublish:
			if publish == nil || !publish(fc, corr, req) {
				ok(fc, corr)
			}
		default:
			return false
		}
		return true
	})
	return st
}

type reqResult struct {
	m   *CoreMessage
	err error
}

func goRequest(c *Client, ctx context.Context, subject string, value string, headers ...Header) chan reqResult {
	ch := make(chan reqResult, 1)
	go func() {
		m, err := c.Request(ctx, subject, []byte(value), headers...)
		ch <- reqResult{m, err}
	}()
	return ch
}

func TestRequestsShareOneInboxAndRouteByToken(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	st := inboxServer(s, nil)
	ctx := testCtx(t)
	a := goRequest(c, ctx, "svc.a", "1")
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 1 })
	b := goRequest(c, ctx, "svc.b", `{"n":2}`, Headers("h", "v")...)
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 2 })
	subs := st.list()
	eq(t, len(subs), 1)
	if !regexp.MustCompile(`^_INBOX\.[0-9a-f]{24}\.\*$`).MatchString(subs[0]) {
		t.Fatal(subs[0])
	}
	prefix := strings.TrimSuffix(subs[0], ".*")
	pubs := s.last().of(proto.CorePublish{})
	pa, pb := pubs[0].req.(proto.CorePublish), pubs[1].req.(proto.CorePublish)
	eq(t, *pa.ReplyTo, prefix+".1")
	eq(t, *pb.ReplyTo, prefix+".2")
	eq(t, pb.Headers, []proto.Header{{Key: "h", Value: "v"}})
	// Out of order, on the inbox subscription.
	s.last().reply(0, proto.CoreMsg{SubID: 0x80000010, Subject: *pb.ReplyTo, Value: []byte("B")})
	s.last().reply(0, proto.CoreMsg{SubID: 0x80000010, Subject: *pa.ReplyTo, Value: []byte("A")})
	ra, rb := <-a, <-b
	if ra.err != nil || rb.err != nil {
		t.Fatal(ra.err, rb.err)
	}
	eq(t, ra.m.Text(), "A")
	eq(t, rb.m.Text(), "B")

	third := goRequest(c, ctx, "svc.a", "3")
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 3 })
	eq(t, len(st.list()), 1) // still the same inbox
	p3 := s.last().of(proto.CorePublish{})[2].req.(proto.CorePublish)
	s.last().reply(0, proto.CoreMsg{SubID: 0x80000010, Subject: *p3.ReplyTo, Value: []byte("C")})
	r3 := <-third
	if r3.err != nil || r3.m.Text() != "C" {
		t.Fatal(r3)
	}
}

func TestRequestWithNoRespondersFailsAtOnce(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	inboxServer(s, func(fc *fakeConn, corr uint32, _ proto.Message) bool {
		fc.reply(corr, proto.Error{Code: 404, Message: "no responders for 'svc.none'"})
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := c.Request(ctx, "svc.none", []byte("x"))
	mustCode(t, err, 404)
	if time.Since(start) > time.Second {
		t.Fatal("should fail at once")
	}
}

func TestRequestTimesOutAndIgnoresLateResponses(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	inboxServer(s, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.Request(ctx, "svc.slow", []byte("x"))
	var te *TimeoutError
	if !errors.As(err, &te) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	p := s.last().of(proto.CorePublish{})[0].req.(proto.CorePublish)
	s.last().reply(0, proto.CoreMsg{SubID: 0x80000010, Subject: *p.ReplyTo, Value: []byte("late")})
	if _, err := c.Ping(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	// The client's request timeout applies too.
	c2 := connectFake(t, s, WithRequestTimeout(100*time.Millisecond))
	if _, err := c2.Request(context.Background(), "svc.slow", []byte("x")); !errors.As(err, &te) {
		t.Fatal(err)
	}
}

func TestInboxEndFailsWaitersAndANewInboxIsUsedNextTime(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	st := inboxServer(s, nil)
	ctx := testCtx(t)
	pending := goRequest(c, ctx, "svc.a", "x")
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 1 })
	s.last().reply(0, proto.SubscriptionEnded{SubID: 0x80000010, Code: 503, Message: "leadership moved"})
	r := <-pending
	mustCode(t, r.err, 503)

	again := goRequest(c, ctx, "svc.a", "y")
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 2 })
	subs := st.list()
	eq(t, len(subs), 2)
	if subs[1] == subs[0] {
		t.Fatal("expected a new inbox")
	}
	p := s.last().of(proto.CorePublish{})[1].req.(proto.CorePublish)
	if !strings.HasPrefix(*p.ReplyTo, strings.TrimSuffix(subs[1], "*")) {
		t.Fatal(*p.ReplyTo)
	}
	s.last().reply(0, proto.CoreMsg{SubID: 0x80000011, Subject: *p.ReplyTo, Value: []byte("ok")})
	if r := <-again; r.err != nil || r.m.Text() != "ok" {
		t.Fatal(r)
	}
}

func TestReconnectResubscribesCoreSubscriptionsAndUsesANewInbox(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var mu sync.Mutex
	id := uint32(0x80000020)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.CoreSubscribe:
			mu.Lock()
			sid := id
			id++
			mu.Unlock()
			fc.reply(corr, proto.SubscribeOk{SubID: sid})
		case proto.CorePublish:
			ok(fc, corr)
		default:
			return false
		}
		return true
	})
	reconnected := make(chan struct{}, 1)
	c := connectFake(t, s, WithReconnect(ReconnectPolicy{InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}),
		WithReconnectHandler(func(ServerInfo) { reconnected <- struct{}{} }))
	ctx := testCtx(t)
	sub, err := c.QueueSubscribeCore(ctx, "events.>", "g")
	if err != nil {
		t.Fatal(err)
	}
	pending := goRequest(c, ctx, "svc.a", "x")
	s.until(func() bool { return len(s.last().of(proto.CorePublish{})) == 1 })
	firstInbox := s.last().of(proto.CoreSubscribe{})[1].req.(proto.CoreSubscribe).Subject

	s.connections()[0].drop()
	var ce *ConnectionError
	if r := <-pending; !errors.As(r.err, &ce) {
		t.Fatal(r.err)
	}
	<-reconnected
	second := s.connections()[1]
	var resubs []proto.Message
	for _, r := range second.of(proto.CoreSubscribe{}) {
		resubs = append(resubs, r.req)
	}
	eq(t, resubs, []proto.Message{proto.CoreSubscribe{Subject: "events.>", Queue: sp("g")}})
	second.reply(0, proto.CoreMsg{SubID: sub.ID(), Subject: "events.x", Value: []byte("after")})
	m, err := nextCore(t, sub, time.Second)
	if err != nil || m.Text() != "after" {
		t.Fatal(m, err)
	}

	again := goRequest(c, ctx, "svc.a", "y")
	s.until(func() bool { return len(second.of(proto.CorePublish{})) == 1 })
	inboxSub := second.of(proto.CoreSubscribe{})[1].req.(proto.CoreSubscribe)
	if inboxSub.Subject == firstInbox {
		t.Fatal("expected a new inbox")
	}
	mu.Lock()
	last := id - 1
	mu.Unlock()
	p := second.of(proto.CorePublish{})[0].req.(proto.CorePublish)
	second.reply(0, proto.CoreMsg{SubID: last, Subject: *p.ReplyTo, Value: []byte("ok")})
	if r := <-again; r.err != nil || r.m.Text() != "ok" {
		t.Fatal(r)
	}
}

// ---- KV ----

func kvRec(offset uint64, key, value, op string) proto.WireRecord {
	r := proto.WireRecord{Offset: offset, TimestampNs: 1_700_000_000_000_000_000 + offset, Subject: key, Value: []byte(value)}
	if op != "" {
		r.Headers = []proto.Header{{Key: "exspeed-kv-op", Value: op}}
	}
	return r
}

func u64p(v uint64) *uint64 { return &v }

func TestKVWritesEncodeAndReturnRevisions(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	var rev uint64
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.KvCreateBucket:
			ok(fc, corr)
		case proto.KvPut, proto.KvDelete:
			rev++
			fc.reply(corr, proto.PublishOk{Offset: rev})
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	kv := c.KV("cfg")
	eq(t, kv.Stream(), "KV_cfg")
	if err := kv.Create(ctx, KVBucketOptions{History: 5, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := c.KV("plain").Create(ctx, KVBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	steps := []func() (uint64, error){
		func() (uint64, error) {
			return kv.PutWith(ctx, "a", []byte(`{"on":true}`), KVPutOptions{TTL: 500 * time.Millisecond})
		},
		func() (uint64, error) { return kv.CreateKey(ctx, "b", []byte("x")) },
		func() (uint64, error) { return kv.Update(ctx, "b", []byte("y"), 2) },
		func() (uint64, error) { return kv.Delete(ctx, "a") },
		func() (uint64, error) {
			return kv.DeleteWith(ctx, "b", KVDeleteOptions{Purge: true, ExpectedRevision: u64p(3)})
		},
		func() (uint64, error) { return kv.Purge(ctx, "c") },
		func() (uint64, error) { return kv.Put(ctx, "d", nil) },
	}
	for i, step := range steps {
		r, err := step()
		if err != nil || r != uint64(i+1) {
			t.Fatalf("step %d: %d %v", i, r, err)
		}
	}
	var reqs []proto.Message
	for _, r := range s.last().received() {
		switch r.req.(type) {
		case proto.KvCreateBucket, proto.KvPut, proto.KvDelete:
			reqs = append(reqs, r.req)
		}
	}
	eq(t, reqs, []proto.Message{
		proto.KvCreateBucket{Bucket: "cfg", History: 5, TTLMs: 60_000},
		proto.KvCreateBucket{Bucket: "plain"},
		proto.KvPut{Bucket: "cfg", Key: "a", Value: []byte(`{"on":true}`), TTLMs: u64p(500)},
		proto.KvPut{Bucket: "cfg", Key: "b", Value: []byte("x"), ExpectedRevision: u64p(0)},
		proto.KvPut{Bucket: "cfg", Key: "b", Value: []byte("y"), ExpectedRevision: u64p(2)},
		proto.KvDelete{Bucket: "cfg", Key: "a"},
		proto.KvDelete{Bucket: "cfg", Key: "b", Purge: true, ExpectedRevision: u64p(3)},
		proto.KvDelete{Bucket: "cfg", Key: "c", Purge: true},
		proto.KvPut{Bucket: "cfg", Key: "d", Value: []byte{}},
	})
}

func TestKVConflictIsAServerError409(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, isPut := req.(proto.KvPut); isPut {
			fc.reply(corr, proto.Error{Code: 409, Message: "wrong revision", Detail: []byte(`{"current_revision":7}`)})
			return true
		}
		return false
	})
	_, err := c.KV("b").Update(testCtx(t), "k", []byte("v"), 3)
	se := mustCode(t, err, 409)
	if !errors.Is(err, ErrConflict) {
		t.Fatal("errors.Is ErrConflict")
	}
	if r, ok := se.CurrentRevision(); !ok || r != 7 {
		t.Fatal(r, ok)
	}
}

func TestKVGetNullVersus404(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		g, isGet := req.(proto.KvGet)
		if !isGet {
			return false
		}
		switch {
		case g.Bucket == "nope":
			fc.reply(corr, proto.Error{Code: 404, Message: "bucket 'nope' not found"})
		case g.Key == "live":
			fc.reply(corr, proto.Messages{Records: []proto.WireRecord{kvRec(4, "live", `{"n":1}`, "")}})
		case g.Key == "gone":
			fc.reply(corr, proto.Error{Code: 404, Message: "key 'gone' not found"})
		case g.Key == "old":
			fc.reply(corr, proto.Messages{Records: []proto.WireRecord{kvRec(*g.Revision-1, "old", "v1", "")}})
		default:
			fc.reply(corr, proto.Messages{})
		}
		return true
	})
	ctx := testCtx(t)
	kv := c.KV("b")
	e, err := kv.Get(ctx, "live")
	if err != nil {
		t.Fatal(err)
	}
	var body struct{ N int }
	_ = e.JSON(&body)
	eq(t, []any{e.Key, e.Revision, e.Op, body.N}, []any{"live", uint64(5), KVPut, 1})
	eq(t, e.Time, time.Unix(0, 1_700_000_000_000_000_004))
	for _, key := range []string{"gone", "empty"} {
		if e, err := kv.Get(ctx, key); e != nil || err != nil {
			t.Fatalf("%s: %v %v", key, e, err)
		}
	}
	old, err := kv.GetRevision(ctx, "old", 2)
	if err != nil || old.Revision != 2 || old.Text() != "v1" {
		t.Fatal(old, err)
	}
	var revs []*uint64
	for _, r := range s.last().of(proto.KvGet{}) {
		revs = append(revs, r.req.(proto.KvGet).Revision)
	}
	eq(t, revs, []*uint64{nil, nil, nil, u64p(2)})
	_, err = c.KV("nope").Get(ctx, "k")
	mustCode(t, err, 404)
}

func TestKVKeysAndHistory(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.KvKeys:
			fc.reply(corr, proto.JSON{Data: []byte(`["a.1","a.2"]`)})
		case proto.KvHistory:
			fc.reply(corr, proto.Messages{Records: []proto.WireRecord{
				kvRec(0, "k", "v1", ""), kvRec(3, "k", "", "DEL"), kvRec(5, "k", "v2", ""), kvRec(6, "k", "", "PURGE"),
			}})
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	kv := c.KV("b")
	keys, err := kv.Keys(ctx, "a.*")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, keys, []string{"a.1", "a.2"})
	_, _ = kv.Keys(ctx, "")
	var filters []string
	for _, r := range s.last().of(proto.KvKeys{}) {
		filters = append(filters, r.req.(proto.KvKeys).Filter)
	}
	eq(t, filters, []string{"a.*", ""})
	h, err := kv.History(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	var got []any
	for _, e := range h {
		got = append(got, e.Revision, e.Op)
	}
	eq(t, got, []any{uint64(1), KVPut, uint64(4), KVDelete, uint64(6), KVPut, uint64(7), KVPurge})
}

func TestKVWatchSnapshotThenChanges(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	var mu sync.Mutex
	var reads []proto.Read
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		r, isRead := req.(proto.Read)
		if !isRead {
			return false
		}
		mu.Lock()
		reads = append(reads, r)
		mu.Unlock()
		switch {
		case r.WaitMs == 0 && r.From == 0: // snapshot, page 1 of 2 (high watermark 5)
			fc.reply(corr, proto.ReadResult{NextOffset: 3, HighWatermark: 5, Records: []proto.WireRecord{
				kvRec(0, "a", "a1", ""), kvRec(1, "b", "b1", ""), kvRec(2, "a", "a2", "")}})
		case r.WaitMs == 0 && r.From == 3:
			fc.reply(corr, proto.ReadResult{NextOffset: 5, HighWatermark: 6, Records: []proto.WireRecord{
				kvRec(3, "c", "c1", ""), kvRec(4, "b", "", "DEL")}})
		case r.From == 5:
			fc.reply(corr, proto.ReadResult{NextOffset: 7, HighWatermark: 7, Records: []proto.WireRecord{
				kvRec(5, "a", "a3", ""), kvRec(6, "c", "", "DEL")}})
		default: // hold the long-poll
		}
		return true
	})
	w := c.KV("b").Watch("x.>")
	var seen []any
	for len(seen) < 12 {
		e, err := w.Next(testCtx(t))
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, e.Key, e.Revision, e.Op)
	}
	// b was deleted before the snapshot ended: left out. a (rev 3) before c (rev 4).
	eq(t, seen, []any{"a", uint64(3), KVPut, "c", uint64(4), KVPut, "a", uint64(6), KVPut, "c", uint64(7), KVDelete})
	w.Stop()
	mu.Lock()
	var got [][]any
	for _, r := range reads[:3] {
		got = append(got, []any{r.Stream, r.From, r.WaitMs, r.Filter})
	}
	mu.Unlock()
	eq(t, got, [][]any{{"KV_b", uint64(0), uint32(0), "x.>"}, {"KV_b", uint64(3), uint32(0), "x.>"}, {"KV_b", uint64(5), uint32(10_000), "x.>"}})
	if !w.Closed() {
		t.Fatal("closed")
	}
	if _, err := w.Next(testCtx(t)); !errors.Is(err, ErrWatchStopped) {
		t.Fatal(err)
	}
}

func TestKVWatchNextTimeoutKeepsTheLongPollResults(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	type held struct {
		fc   *fakeConn
		corr uint32
	}
	heldCh := make(chan held, 1)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		r, isRead := req.(proto.Read)
		if !isRead {
			return false
		}
		if r.WaitMs == 0 {
			fc.reply(corr, proto.ReadResult{})
		} else {
			heldCh <- held{fc, corr}
		}
		return true
	})
	w := c.KV("b").Watch("")
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := w.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	h := <-heldCh
	h.fc.reply(h.corr, proto.ReadResult{NextOffset: 1, HighWatermark: 1, Records: []proto.WireRecord{kvRec(0, "k", "v", "")}})
	e, err := w.Next(testCtx(t))
	if err != nil || e.Key != "k" || e.Revision != 1 {
		t.Fatal(e, err)
	}
	eq(t, len(s.last().of(proto.Read{})) >= 2, true)
}

// ---- consumer info ----

func TestConsumerInfoDecodesEveryField(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	info := `{"spec":{"name":"c","stream":"s","filter_headers":{"x_tenant_id":"acme"},"header_match":"any",` +
		`"single_active":true,"priority_window":5,"dead_letter_expired":true,"max_deliver":5,"deliver":"new"},` +
		`"next_offset":3,"ack_floor":2,"num_unacked":1,"num_in_flight":1,"num_delayed":2,"num_waiting":4,"lag":5,` +
		`"subscribers":1,"pull_waiters":0,"stats":{"delivered":3,"redelivered":1,"acked":2,"dead_lettered":1,"gone":0,"skipped":0}}`
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.ConsumerInfo, proto.CreateConsumer:
			fc.reply(corr, proto.JSON{Data: []byte(info)})
		case proto.ListConsumers:
			fc.reply(corr, proto.JSON{Data: []byte(`[` + info + `,{"spec":{"name":"d","stream":"s"}}]`)})
		case proto.DeleteConsumer, proto.SeekConsumer:
			ok(fc, corr)
		default:
			return false
		}
		return true
	})
	ctx := testCtx(t)
	i, err := c.ConsumerInfo(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, i.Spec.FilterHeaders, map[string]string{"x_tenant_id": "acme"})
	eq(t, []any{i.Spec.HeaderMatch, i.Spec.SingleActive, i.Spec.PriorityWindow, i.Spec.DeadLetterExpired, i.Spec.MaxDeliver, i.Spec.Deliver.String()},
		[]any{HeaderMatchAny, true, 5, true, 5, "new"})
	eq(t, []uint64{i.NextOffset, i.AckFloor, i.NumUnacked, i.NumInFlight, i.NumDelayed, i.NumWaiting, i.Lag, i.Subscribers},
		[]uint64{3, 2, 1, 1, 2, 4, 5, 1})
	eq(t, i.Stats, ConsumerStats{Delivered: 3, Redelivered: 1, Acked: 2, DeadLettered: 1})

	created, err := c.CreateConsumer(ctx, ConsumerSpec{Name: "c", Stream: "s", FilterHeaders: map[string]string{"x_tenant_id": "acme"}, HeaderMatch: HeaderMatchAny})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, created.Spec.FilterHeaders, map[string]string{"x_tenant_id": "acme"})
	eq(t, string(s.last().of(proto.CreateConsumer{})[0].req.(proto.CreateConsumer).Spec),
		`{"name":"c","stream":"s","filter_headers":{"x_tenant_id":"acme"},"header_match":"any"}`)
	list, err := c.ListConsumers(ctx, "s")
	if err != nil || len(list) != 2 || list[1].Spec.Name != "d" || list[1].Spec.FilterHeaders != nil {
		t.Fatal(list, err)
	}
	eq(t, s.last().of(proto.ListConsumers{})[0].req, proto.Message(proto.ListConsumers{Stream: sp("s")}))
	_, _ = c.ListConsumers(ctx, "")
	eq(t, s.last().of(proto.ListConsumers{})[1].req, proto.Message(proto.ListConsumers{}))

	if err := c.DeleteConsumer(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	for _, target := range []SeekTarget{SeekEarliest(), SeekLatest(), SeekOffset(1000), SeekTime(time.UnixMilli(9))} {
		if err := c.Seek(ctx, "c", target); err != nil {
			t.Fatal(err)
		}
	}
	var seeks []proto.Message
	for _, r := range s.last().of(proto.SeekConsumer{}) {
		seeks = append(seeks, r.req)
	}
	eq(t, seeks, []proto.Message{
		proto.SeekConsumer{Consumer: "c", Kind: proto.SeekEarliest},
		proto.SeekConsumer{Consumer: "c", Kind: proto.SeekLatest},
		proto.SeekConsumer{Consumer: "c", Kind: proto.SeekOffset, Value: 1000},
		proto.SeekConsumer{Consumer: "c", Kind: proto.SeekTime, Value: 9},
	})
}

// ---- publisher ----

var nextOffsetMu sync.Mutex

// autoAck answers publishes with increasing offsets.
func autoAck(next *uint64) handlerFunc {
	return func(fc *fakeConn, corr uint32, req proto.Message) bool {
		nextOffsetMu.Lock()
		defer nextOffsetMu.Unlock()
		switch r := req.(type) {
		case proto.Publish:
			fc.reply(corr, proto.PublishOk{Offset: *next})
			*next++
		case proto.PublishBatch:
			res := make([]proto.PublishResult, len(r.Records))
			for i := range res {
				res[i].Offset = *next
				*next++
			}
			fc.reply(corr, proto.PublishBatchOk{Results: res})
		default:
			return false
		}
		return true
	}
}

func subjectsOf(reqs []received) []string {
	var out []string
	for _, r := range reqs {
		switch m := r.req.(type) {
		case proto.Publish:
			out = append(out, m.Record.Subject)
		case proto.PublishBatch:
			for _, x := range m.Records {
				out = append(out, x.Subject)
			}
		}
	}
	return out
}

func numbered(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + "." + strconv.Itoa(i)
	}
	return out
}

func TestPublisherCoalescesIntoOneBatchInCallOrder(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var off uint64
	s.setHandler(autoAck(&off))
	c := connectFake(t, s)
	p := c.NewPublisher(PublisherOptions{BatchWindow: 200 * time.Millisecond})
	ctx := testCtx(t)
	var acks []*PubAck
	for _, subj := range numbered("n", 100) {
		a, err := p.PublishAsync(ctx, "s", PublishRecord{Subject: subj})
		if err != nil {
			t.Fatal(err)
		}
		acks = append(acks, a)
	}
	for i, a := range acks {
		r, err := a.Result()
		if err != nil || r.Offset != uint64(i) {
			t.Fatal(i, r, err)
		}
	}
	eq(t, s.last().types(), []string{"PublishBatch"})
	eq(t, subjectsOf(s.last().received()), numbered("n", 100))
}

func TestPublisherSendsALoneRecordAsPublish(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var off uint64
	s.setHandler(autoAck(&off))
	c := connectFake(t, s)
	r, err := c.NewPublisher(PublisherOptions{}).Publish(testCtx(t), "s", PublishRecord{Subject: "one", Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, PublishResult{Offset: 0})
	eq(t, s.last().types(), []string{"Publish"})
}

func TestPublisherSplitsByBatchSizeAndStreamKeepingOrder(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var off uint64
	s.setHandler(autoAck(&off))
	c := connectFake(t, s)
	p := c.NewPublisher(PublisherOptions{MaxBatchRecords: 3, BatchWindow: time.Hour})
	ctx := testCtx(t)
	var acks []*PubAck
	add := func(stream, subj string) {
		a, err := p.PublishAsync(ctx, stream, PublishRecord{Subject: subj})
		if err != nil {
			t.Fatal(err)
		}
		acks = append(acks, a)
	}
	// A full queue (3 records) wakes the publisher at once.
	add("a", "a.0")
	add("a", "a.1")
	add("a", "a.2")
	for _, a := range acks {
		if _, err := a.Result(); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, s.last().types(), []string{"PublishBatch"})
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Runs of the same stream.
	q := c.NewPublisher(PublisherOptions{MaxBatchRecords: 3, BatchWindow: 100 * time.Millisecond})
	acks = nil
	for _, x := range [][2]string{{"a", "a.3"}, {"b", "b.0"}, {"b", "b.1"}} {
		a, err := q.PublishAsync(ctx, x[0], PublishRecord{Subject: x[1]})
		if err != nil {
			t.Fatal(err)
		}
		acks = append(acks, a)
	}
	for _, a := range acks {
		if _, err := a.Result(); err != nil {
			t.Fatal(err)
		}
	}
	got := s.last().received()[1:]
	var shape []string
	for _, r := range got {
		switch m := r.req.(type) {
		case proto.Publish:
			shape = append(shape, "Publish:"+m.Stream+":1")
		case proto.PublishBatch:
			shape = append(shape, "PublishBatch:"+m.Stream+":"+strconv.Itoa(len(m.Records)))
		}
	}
	eq(t, shape, []string{"PublishBatch:a:3", "Publish:a:1", "PublishBatch:b:2"})
	eq(t, subjectsOf(got), []string{"a.0", "a.1", "a.2", "a.3", "b.0", "b.1"})
}

func TestPublisherBoundsRecordsInFlight(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var off uint64
	ack := autoAck(&off)
	var hmu sync.Mutex
	var held []func()
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Publish, proto.PublishBatch:
			hmu.Lock()
			held = append(held, func() { ack(fc, corr, req) })
			hmu.Unlock()
			return true
		}
		return false
	})
	c := connectFake(t, s)
	p := c.NewPublisher(PublisherOptions{MaxInFlight: 2})
	ctx := testCtx(t)
	results := make(chan error, 5)
	go func() {
		for _, subj := range numbered("n", 5) {
			a, err := p.PublishAsync(ctx, "s", PublishRecord{Subject: subj})
			if err != nil {
				results <- err
				continue
			}
			go func() { _, err := a.Result(); results <- err }()
		}
	}()
	waitFor(t, func() bool { return p.Pending() == 2 })
	time.Sleep(30 * time.Millisecond)
	eq(t, p.Pending(), 2) // the third waits for a permit
	for done := 0; done < 5; {
		hmu.Lock()
		h := held
		held = nil
		hmu.Unlock()
		for _, f := range h {
			f()
		}
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
			done++
		case <-time.After(5 * time.Millisecond):
		}
	}
	eq(t, subjectsOf(s.last().received()), numbered("n", 5))
	if err := p.Flush(ctx); err != nil || p.Pending() != 0 {
		t.Fatal(err, p.Pending())
	}
}

func TestPublisherFailsEveryRecordOfAFailedBatch(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, isBatch := req.(proto.PublishBatch); isBatch {
			fc.reply(corr, proto.Error{Code: 403, Message: "forbidden"})
			return true
		}
		return false
	})
	c := connectFake(t, s)
	p := c.NewPublisher(PublisherOptions{BatchWindow: 50 * time.Millisecond})
	ctx := testCtx(t)
	a1, _ := p.PublishAsync(ctx, "s", PublishRecord{Subject: "a"})
	a2, _ := p.PublishAsync(ctx, "s", PublishRecord{Subject: "b"})
	for _, a := range []*PubAck{a1, a2} {
		_, err := a.Wait(ctx)
		mustCode(t, err, 403)
	}
	if err := p.Flush(ctx); err != nil || p.Pending() != 0 {
		t.Fatal(err)
	}
}

func TestPublisherFlushAndClose(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var off uint64
	s.setHandler(autoAck(&off))
	c := connectFake(t, s)
	p := c.NewPublisher(PublisherOptions{})
	ctx := testCtx(t)
	for i := 0; i < 10; i++ {
		if _, err := p.PublishAsync(ctx, "s", PublishRecord{Subject: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(ctx); err != nil || p.Pending() != 0 {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Publish(ctx, "s", PublishRecord{Subject: "x"}); !errors.Is(err, ErrPublisherClosed) {
		t.Fatal(err)
	}
}
