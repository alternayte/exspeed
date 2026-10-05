package exspeed_test

// End-to-end tests of stream limits, time headers, routing settings, core
// pub/sub, request-reply and KV buckets against a real exspeed server.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	exspeed "github.com/alternayte/exspeed/sdks/go"
)

func texts(msgs []*exspeed.Message) string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Text())
	}
	return fmt.Sprint(out)
}

func recordTexts(r *exspeed.ReadResult) string {
	var out []string
	for _, x := range r.Records {
		out = append(out, x.Text())
	}
	return fmt.Sprint(out)
}

func TestE2ELimitsAndTimeHeaders(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t, exspeed.WithClientID("e2e-messaging"))
	ctx := ctxFor(t, 60*time.Second)

	t.Run("expired records are hidden from reads and consumers", func(t *testing.T) {
		s := uniq("ttl")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s, AllowMsgTTL: true}))
		check(t, must(c.StreamInfo(ctx, s))(t).Config.AllowMsgTTL, "allow_msg_ttl")
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "jobs.a", Value: []byte("short"), TTL: 150 * time.Millisecond}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "jobs.a", Value: []byte("keep")}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "jobs.a", Value: []byte("long"), TTL: time.Hour}))(t)
		cons := uniq("ttl-c")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		time.Sleep(400 * time.Millisecond)
		check(t, recordTexts(must(c.Read(ctx, s, exspeed.ReadOptions{}))(t)) == "[keep long]", "read")
		got := must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 10, Expires: 500 * time.Millisecond}))(t)
		check(t, texts(got) == "[keep long]", "pull %s", texts(got))
		h, _ := got[1].Header(exspeed.TTLHeader)
		check(t, h == "3600000ms", "ttl header %q", h)
	})

	t.Run("time headers need the stream's permission", func(t *testing.T) {
		s := newStream(t, c, "plain")
		for _, r := range []exspeed.PublishRecord{
			{Subject: "a.b", Value: []byte("x"), TTL: time.Second},
			{Subject: "a.b", Value: []byte("x"), Delay: time.Second},
		} {
			_, err := c.Publish(ctx, s, r)
			check(t, code(t, err) == 400, "%v", err)
		}
	})

	t.Run("delayed delivery", func(t *testing.T) {
		s := uniq("delay")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s, AllowDelayed: true}))
		cons := uniq("delay-c")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "later.a", Value: []byte("delayed"), Delay: 700 * time.Millisecond}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "later.a", Value: []byte("at"), DeliverAt: time.Now().Add(900 * time.Millisecond)}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "later.a", Value: []byte("now")}))(t)
		first := must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 10, Expires: 300 * time.Millisecond}))(t)
		check(t, texts(first) == "[now]", "first %s", texts(first))
		noErr(t, c.Ack(ctx, cons, first[0].Offset))
		check(t, must(c.ConsumerInfo(ctx, cons))(t).NumDelayed == 2, "num_delayed")
		var due []string
		eventually(t, 10*time.Second, func() bool {
			for _, m := range must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 10, Expires: 200 * time.Millisecond}))(t) {
				due = append(due, m.Text())
				m.Ack()
			}
			return len(due) == 2
		})
		check(t, fmt.Sprint(due) == "[delayed at]", "due %v", due)
		// A stateless read sees every record at once: delays apply to consumers.
		check(t, recordTexts(must(c.Read(ctx, s, exspeed.ReadOptions{}))(t)) == "[delayed at now]", "read")
	})

	t.Run("max msgs drops the oldest or rejects new ones", func(t *testing.T) {
		old := uniq("max-old")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: old, MaxMsgs: 2}))
		for _, v := range []string{"1", "2", "3"} {
			must(c.Publish(ctx, old, exspeed.PublishRecord{Subject: "m.a", Value: []byte(v)}))(t)
		}
		check(t, recordTexts(must(c.Read(ctx, old, exspeed.ReadOptions{}))(t)) == "[2 3]", "discard old")

		strict := uniq("max-new")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: strict, MaxMsgs: 2, Discard: exspeed.DiscardNew}))
		must(c.Publish(ctx, strict, exspeed.PublishRecord{Subject: "m.a", Value: []byte("1")}))(t)
		must(c.Publish(ctx, strict, exspeed.PublishRecord{Subject: "m.a", Value: []byte("2")}))(t)
		_, err := c.Publish(ctx, strict, exspeed.PublishRecord{Subject: "m.a", Value: []byte("3")})
		check(t, code(t, err) == 429 && errors.Is(err, exspeed.ErrTooManyRequests), "%v", err)
		check(t, recordTexts(must(c.Read(ctx, strict, exspeed.ReadOptions{}))(t)) == "[1 2]", "discard new")
		cfg := must(c.StreamInfo(ctx, strict))(t).Config
		check(t, cfg.MaxMsgs == 2 && cfg.Discard == exspeed.DiscardNew, "%+v", cfg)
	})

	t.Run("max msgs per subject, capture subjects and work-queue retention", func(t *testing.T) {
		s := uniq("per-subj")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s, MaxMsgsPerSubject: 1}))
		for _, v := range []string{"a1", "a2"} {
			must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "k.a", Value: []byte(v)}))(t)
		}
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "k.b", Value: []byte("b1")}))(t)
		check(t, recordTexts(must(c.Read(ctx, s, exspeed.ReadOptions{}))(t)) == "[a2 b1]", "per subject")

		// Core messages to captured subjects are appended to the stream.
		capt := uniq("capture")
		subj := "cap" + strings.ReplaceAll(uniq(""), "-", "")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: capt, CaptureSubjects: []string{subj + ".>"}}))
		check(t, fmt.Sprint(must(c.StreamInfo(ctx, capt))(t).Config.CaptureSubjects) == "["+subj+".>]", "capture_subjects in info")
		noErr(t, c.PublishCore(ctx, subj+".x", []byte("captured"), exspeed.Headers("h", "1")...))
		noErr(t, c.PublishCore(ctx, "other.x", []byte("not captured")))
		var got *exspeed.ReadResult
		eventually(t, 10*time.Second, func() bool {
			got = must(c.Read(ctx, capt, exspeed.ReadOptions{}))(t)
			return len(got.Records) > 0
		})
		check(t, len(got.Records) == 1 && got.Records[0].Subject == subj+".x" && got.Records[0].Text() == "captured", "%+v", got.Records)

		wq := uniq("wq")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: wq, Retention: exspeed.RetentionWorkQueue}))
		check(t, must(c.StreamInfo(ctx, wq))(t).Config.Retention == exspeed.RetentionWorkQueue, "retention")
	})
}

func TestE2EConsumerRouting(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t)
	ctx := ctxFor(t, 30*time.Second)

	t.Run("header filters (all / any)", func(t *testing.T) {
		s := newStream(t, c, "hdr")
		for _, x := range [][3]string{{"eu", "gold", "a"}, {"us", "gold", "b"}, {"eu", "free", "c"}, {"asia", "free", "d"}} {
			must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "e.x", Value: []byte(x[2]), Headers: exspeed.Headers("region", x[0], "tier", x[1])}))(t)
		}
		all := uniq("hdr-all")
		info := must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: all, Stream: s, FilterHeaders: map[string]string{"region": "eu", "tier": "gold"}}))(t)
		check(t, fmt.Sprint(info.Spec.FilterHeaders) == "map[region:eu tier:gold]", "%v", info.Spec.FilterHeaders)
		check(t, texts(must(c.Pull(ctx, all, exspeed.PullOptions{Expires: 300 * time.Millisecond}))(t)) == "[a]", "all")
		anyc := uniq("hdr-any")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: anyc, Stream: s, FilterHeaders: map[string]string{"region": "eu", "tier": "gold"}, HeaderMatch: exspeed.HeaderMatchAny}))(t)
		check(t, texts(must(c.Pull(ctx, anyc, exspeed.PullOptions{Expires: 300 * time.Millisecond}))(t)) == "[a b c]", "any")
	})

	t.Run("higher priorities first within the window", func(t *testing.T) {
		s := newStream(t, c, "prio")
		for _, x := range []struct {
			v string
			p int
		}{{"low1", 0}, {"high1", 9}, {"mid", 5}, {"low2", 0}, {"high2", 9}} {
			must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "t.x", Value: []byte(x.v), Priority: x.p}))(t)
		}
		cons := uniq("prio-c")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, PriorityWindow: 100}))(t)
		var order []string
		for len(order) < 5 {
			got := must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 10, Expires: 500 * time.Millisecond}))(t)
			for _, m := range got {
				order = append(order, m.Text())
				noErr(t, m.AckSync(ctx))
			}
		}
		check(t, fmt.Sprint(order) == "[high1 high2 mid low1 low2]", "%v", order)
	})

	t.Run("single active consumers refuse pulls", func(t *testing.T) {
		s := newStream(t, c, "single")
		cons := uniq("single-c")
		info := must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, SingleActive: true, DeadLetterExpired: true}))(t)
		check(t, info.Spec.SingleActive && info.Spec.DeadLetterExpired, "%+v", info.Spec)
		_, err := c.Pull(ctx, cons, exspeed.PullOptions{NoWait: true})
		check(t, code(t, err) == 400, "pull on a single-active consumer: %v", err)
	})
}

func nextCoreMsg(t *testing.T, s *exspeed.CoreSubscription, d time.Duration) *exspeed.CoreMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	m, err := s.Next(ctx)
	if err != nil {
		return nil
	}
	return m
}

func TestE2ECoreMessaging(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t)
	ctx := ctxFor(t, 60*time.Second)

	t.Run("fans out to every matching subscription and stores nothing", func(t *testing.T) {
		a, b := srv.connect(t), srv.connect(t)
		all := must(a.SubscribeCore(ctx, "orders.>"))(t)
		eu := must(b.SubscribeCore(ctx, "orders.eu.*"))(t)
		noErr(t, c.PublishCore(ctx, "orders.eu.created", []byte(`{"id":1}`), exspeed.Headers("trace-id", "t1")...))
		noErr(t, c.PublishCore(ctx, "orders.us.created", []byte("2")))
		noErr(t, c.PublishCore(ctx, "billing.x", []byte("ignored")))

		m1 := nextCoreMsg(t, all, 5*time.Second)
		h, _ := m1.Header("trace-id")
		check(t, m1.Subject == "orders.eu.created" && string(m1.Value) == `{"id":1}` && h == "t1" && m1.ReplyTo == "", "%+v", m1)
		check(t, nextCoreMsg(t, all, 5*time.Second).Subject == "orders.us.created", "second")
		check(t, nextCoreMsg(t, eu, 5*time.Second).Subject == "orders.eu.created", "eu")
		check(t, nextCoreMsg(t, eu, 200*time.Millisecond) == nil, "eu extra")
		check(t, nextCoreMsg(t, all, 200*time.Millisecond) == nil, "all extra")

		// A late subscriber sees only what comes next.
		late := must(a.SubscribeCore(ctx, "orders.>"))(t)
		check(t, nextCoreMsg(t, late, 200*time.Millisecond) == nil, "late")
		noErr(t, late.Unsubscribe(ctx))
		noErr(t, all.Unsubscribe(ctx))
		noErr(t, c.PublishCore(ctx, "orders.eu.created", []byte("3")))
		check(t, nextCoreMsg(t, eu, 5*time.Second).Text() == "3", "after unsubscribe")
		check(t, all.Closed(), "closed")
	})

	t.Run("queue groups split messages", func(t *testing.T) {
		w1, w2 := srv.connect(t), srv.connect(t)
		subject := "jobs." + uniq("q")
		s1 := must(w1.QueueSubscribeCore(ctx, subject, "workers"))(t)
		s2 := must(w2.QueueSubscribeCore(ctx, subject, "workers"))(t)
		for i := 0; i < 20; i++ {
			noErr(t, c.PublishCore(ctx, subject, []byte(fmt.Sprint(i))))
		}
		count := func(s *exspeed.CoreSubscription) int {
			n := 0
			for nextCoreMsg(t, s, 300*time.Millisecond) != nil {
				n++
			}
			return n
		}
		n1, n2 := count(s1), count(s2)
		check(t, n1+n2 == 20 && n1 > 0 && n2 > 0, "n1=%d n2=%d", n1, n2)
	})

	t.Run("request-reply through one inbox; no responders fails fast", func(t *testing.T) {
		svc := srv.connect(t)
		reqs := must(svc.QueueSubscribeCore(ctx, "svc.upper", "svc"))(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for m := range reqs.Messages() {
				_ = m.Respond(ctx, []byte(strings.ToUpper(m.Text())))
			}
		}()
		r := must(c.Request(ctx, "svc.upper", []byte("hello")))(t)
		check(t, r.Text() == "HELLO", "%q", r.Text())
		results := make([]chan string, 20)
		for i := range results {
			results[i] = make(chan string, 1)
			go func(i int) {
				m, err := c.Request(ctx, "svc.upper", []byte(fmt.Sprintf("m%d", i)))
				if err != nil {
					results[i] <- err.Error()
					return
				}
				results[i] <- m.Text()
			}(i)
		}
		for i, ch := range results {
			check(t, <-ch == fmt.Sprintf("M%d", i), "response %d", i)
		}

		start := time.Now()
		_, err := c.Request(ctx, "svc.nobody", []byte("x"))
		check(t, code(t, err) == 404 && time.Since(start) < 2*time.Second, "%v after %v", err, time.Since(start))
		noErr(t, reqs.Unsubscribe(ctx))
		<-done
	})

	t.Run("request times out when the responder never answers", func(t *testing.T) {
		svc := srv.connect(t)
		subject := "svc." + uniq("silent")
		silent := must(svc.SubscribeCore(ctx, subject))(t)
		rctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		_, err := c.Request(rctx, subject, []byte("x"))
		var te *exspeed.TimeoutError
		check(t, errors.As(err, &te), "%v", err)
		m := nextCoreMsg(t, silent, time.Second)
		check(t, m != nil && regexp.MustCompile(`^_INBOX\.`).MatchString(m.ReplyTo), "%+v", m)
	})
}

func TestE2EKV(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t)
	ctx := ctxFor(t, 60*time.Second)

	t.Run("put, get, compare-and-set, delete and keys", func(t *testing.T) {
		kv := c.KV(uniq("cfg"))
		noErr(t, kv.Create(ctx, exspeed.KVBucketOptions{History: 3}))
		noErr(t, kv.Create(ctx, exspeed.KVBucketOptions{History: 3})) // idempotent
		check(t, must(kv.Get(ctx, "app.mode"))(t) == nil, "absent")

		r1 := must(kv.Put(ctx, "app.mode", []byte("dev")))(t)
		r2 := must(kv.Put(ctx, "app.mode", []byte(`{"mode":"prod"}`)))(t)
		check(t, r1 == 1 && r2 == 2, "%d %d", r1, r2)
		e := must(kv.Get(ctx, "app.mode"))(t)
		check(t, e.Key == "app.mode" && e.Revision == r2 && e.Op == exspeed.KVPut && e.Text() == `{"mode":"prod"}`, "%+v", e)
		check(t, must(kv.GetRevision(ctx, "app.mode", r1))(t).Text() == "dev", "revision 1")

		created := must(kv.CreateKey(ctx, "app.port", []byte("8080")))(t)
		_, err := kv.CreateKey(ctx, "app.port", []byte("9090"))
		check(t, code(t, err) == 409, "%v", err)
		updated := must(kv.Update(ctx, "app.port", []byte("9090"), created))(t)
		_, err = kv.Update(ctx, "app.port", []byte("1"), created)
		check(t, code(t, err) == 409, "%v", err)
		cur, ok := err.(*exspeed.ServerError).CurrentRevision()
		check(t, ok && cur == updated, "current revision %d %v", cur, ok)

		must(kv.Put(ctx, "db.url", []byte("postgres://")))(t)
		check(t, fmt.Sprint(must(kv.Keys(ctx, ""))(t)) == "[app.mode app.port db.url]", "keys")
		check(t, fmt.Sprint(must(kv.Keys(ctx, "app.*"))(t)) == "[app.mode app.port]", "filtered keys")

		del := must(kv.Delete(ctx, "app.port"))(t)
		check(t, del > updated, "tombstone revision")
		check(t, must(kv.Get(ctx, "app.port"))(t) == nil, "deleted")
		check(t, fmt.Sprint(must(kv.Keys(ctx, "app.*"))(t)) == "[app.mode]", "keys after delete")
		var hist []string
		for _, h := range must(kv.History(ctx, "app.port"))(t) {
			hist = append(hist, h.Text()+"/"+string(h.Op))
		}
		check(t, fmt.Sprint(hist) == "[8080/put 9090/put /delete]", "%v", hist)
		// A deleted key can be created again.
		check(t, must(kv.CreateKey(ctx, "app.port", []byte("7070")))(t) > del, "recreate")
		// Deletes take an expected revision too.
		_, err = kv.DeleteWith(ctx, "app.port", exspeed.KVDeleteOptions{ExpectedRevision: &created})
		check(t, code(t, err) == 409, "%v", err)

		must(kv.Purge(ctx, "app.mode"))(t)
		check(t, must(kv.Get(ctx, "app.mode"))(t) == nil, "purged")

		_, err = c.KV(uniq("missing")).Get(ctx, "x")
		check(t, code(t, err) == 404, "%v", err)
		noErr(t, kv.Destroy(ctx))
		_, err = c.StreamInfo(ctx, kv.Stream())
		check(t, code(t, err) == 404, "%v", err)
	})

	t.Run("keys expire after their TTL", func(t *testing.T) {
		kv := c.KV(uniq("ttl"))
		noErr(t, kv.Create(ctx, exspeed.KVBucketOptions{}))
		must(kv.PutWith(ctx, "session.a", []byte("x"), exspeed.KVPutOptions{TTL: 200 * time.Millisecond}))(t)
		must(kv.Put(ctx, "session.b", []byte("y")))(t)
		time.Sleep(500 * time.Millisecond)
		check(t, must(kv.Get(ctx, "session.a"))(t) == nil, "expired")
		check(t, must(kv.Get(ctx, "session.b"))(t).Text() == "y", "kept")
	})

	t.Run("watch: current values first, then every change", func(t *testing.T) {
		kv := c.KV(uniq("watch"))
		noErr(t, kv.Create(ctx, exspeed.KVBucketOptions{}))
		for _, x := range [][2]string{{"user.1", "alice"}, {"user.2", "bob"}, {"user.1", "alice2"}, {"other.x", "filtered"}, {"user.3", "gone"}} {
			must(kv.Put(ctx, x[0], []byte(x[1])))(t)
		}
		must(kv.Delete(ctx, "user.3"))(t)

		w := kv.Watch("user.*")
		defer w.Stop()
		take := func(n int) []*exspeed.KVEntry {
			var out []*exspeed.KVEntry
			for len(out) < n {
				e, err := w.Next(ctxFor(t, 5*time.Second))
				if err != nil {
					t.Fatalf("watch stalled after %d entries: %v", len(out), err)
				}
				out = append(out, e)
			}
			return out
		}
		var snap []string
		for _, e := range take(2) {
			snap = append(snap, fmt.Sprintf("%s=%s@%d", e.Key, e.Text(), e.Revision))
		}
		check(t, fmt.Sprint(snap) == "[user.2=bob@2 user.1=alice2@3]", "%v", snap)

		must(kv.Put(ctx, "user.4", []byte("dave")))(t)
		must(kv.Put(ctx, "other.y", []byte("filtered")))(t)
		must(kv.Delete(ctx, "user.2"))(t)
		var changes []string
		for _, e := range take(2) {
			changes = append(changes, e.Key+"/"+string(e.Op))
		}
		check(t, fmt.Sprint(changes) == "[user.4/put user.2/delete]", "%v", changes)
		short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err := w.Next(short)
		check(t, errors.Is(err, context.DeadlineExceeded), "%v", err)
		w.Stop()
		_, err = w.Next(ctx)
		check(t, errors.Is(err, exspeed.ErrWatchStopped), "%v", err)
	})
}
