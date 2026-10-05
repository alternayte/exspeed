package exspeed_test

// End-to-end tests against a real exspeed server (see e2e_harness_test.go
// for how the binary is found). Skipped when no binary is available.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	exspeed "github.com/alternayte/exspeed/sdks/go"
)

func code(t *testing.T, err error) int {
	t.Helper()
	var se *exspeed.ServerError
	if !errors.As(err, &se) {
		t.Fatalf("want a ServerError, got %v", err)
	}
	return se.Code
}

// must(f())(t) fails the test when f returns an error, else returns f's value.
func must[T any](v T, err error) func(*testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func check(t *testing.T, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(format, args...)
	}
}

func newStream(t *testing.T, c *exspeed.Client, prefix string) string {
	t.Helper()
	name := uniq(prefix)
	noErr(t, c.CreateStream(ctxFor(t, 10*time.Second), exspeed.StreamSpec{Name: name}))
	return name
}

func jsonValue(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func publishN(t *testing.T, c *exspeed.Client, stream string, n int, subject string) {
	t.Helper()
	recs := make([]exspeed.PublishRecord, n)
	for i := range recs {
		recs[i] = exspeed.PublishRecord{Subject: subject, Value: jsonValue(map[string]int{"i": i})}
	}
	must(c.PublishBatch(ctxFor(t, 10*time.Second), stream, recs))(t)
}

func nextMsg(t *testing.T, s *exspeed.Subscription, d time.Duration) *exspeed.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	m, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestE2EBasics(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t, exspeed.WithClientID("e2e"))
	ctx := ctxFor(t, 30*time.Second)

	t.Run("ping and metadata", func(t *testing.T) {
		must(c.Ping(ctx))(t)
		md := must(c.Metadata(ctx))(t)
		check(t, md.IsLeader, "not leader")
		check(t, md.ServerVersion == c.ServerInfo().ServerVersion && md.NodeID == c.ServerInfo().NodeID, "%+v", md)
	})

	t.Run("stream admin", func(t *testing.T) {
		name := uniq("admin")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: name, MaxAge: time.Hour}))
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: name, MaxAge: time.Hour})) // same settings
		err := c.CreateStream(ctx, exspeed.StreamSpec{Name: name, MaxAge: time.Minute})
		check(t, code(t, err) == 409 && errors.Is(err, exspeed.ErrConflict), "%v", err)

		must(c.Publish(ctx, name, exspeed.PublishRecord{Subject: "admin.created", Value: []byte(`{"n":1}`)}))(t)
		info := must(c.StreamInfo(ctx, name))(t)
		check(t, info.Name == name && info.EarliestOffset == 0 && info.NextOffset == 1 && info.Records == 1 && !info.Internal, "%+v", info)
		check(t, info.Config.MaxAgeSecs == 3600, "%+v", info.Config)
		var names []string
		for _, s := range must(c.ListStreams(ctx))(t) {
			names = append(names, s.Name)
		}
		check(t, contains(names, name), "%v", names)

		noErr(t, c.UpdateStream(ctx, exspeed.StreamSpec{Name: name, MaxAge: 2 * time.Hour}))
		check(t, must(c.StreamInfo(ctx, name))(t).Config.MaxAgeSecs == 7200, "update")

		cons := uniq("admin-c")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: name}))(t)
		err = c.DeleteStream(ctx, name)
		check(t, code(t, err) == 409, "%v", err)
		var detail struct{ Consumers []string }
		noErr(t, err.(*exspeed.ServerError).DecodeDetail(&detail))
		check(t, len(detail.Consumers) == 1 && detail.Consumers[0] == cons, "%+v", detail)
		noErr(t, c.DeleteConsumer(ctx, cons))
		noErr(t, c.DeleteStream(ctx, name))
		_, err = c.StreamInfo(ctx, name)
		check(t, errors.Is(err, exspeed.ErrNotFound), "%v", err)
	})

	t.Run("internal names and unknown streams", func(t *testing.T) {
		check(t, code(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: "__nope"})) == 403, "internal")
		_, err := c.Publish(ctx, uniq("missing"), exspeed.PublishRecord{Subject: "x.y", Value: []byte("v")})
		check(t, code(t, err) == 404, "%v", err)
	})

	t.Run("query", func(t *testing.T) {
		s := strings.ReplaceAll(uniq("q"), "-", "_")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s}))
		recs := []exspeed.PublishRecord{}
		for i := 1; i <= 3; i++ {
			recs = append(recs, exspeed.PublishRecord{Subject: "metrics.cpu", Value: jsonValue(map[string]any{"i": i})})
		}
		must(c.PublishBatch(ctx, s, recs))(t)
		r := must(c.Query(ctx, fmt.Sprintf(`SELECT COUNT(*) AS cnt FROM "%s"`, s)))(t)
		check(t, len(r.Columns) == 1 && r.Columns[0] == "cnt" && r.RowCount == 1, "%+v", r)
		check(t, fmt.Sprint(r.Rows) == "[[3]]", "%v", r.Rows)
		_, err := c.Query(ctx, "SELEKT nonsense")
		check(t, code(t, err) == 400, "%v", err)
	})
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func TestE2EPublishingAndReading(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t)
	ctx := ctxFor(t, 60*time.Second)

	t.Run("publish and read with subject filters", func(t *testing.T) {
		s := newStream(t, c, "orders")
		subjects := []string{"orders.placed", "orders.shipped", "orders.eu.placed", "payments.done", "orders.placed"}
		for i, subj := range subjects {
			r := must(c.Publish(ctx, s, exspeed.PublishRecord{
				Subject: subj, Value: jsonValue(map[string]int{"i": i}), Key: []byte(fmt.Sprintf("k%d", i)),
				Headers: exspeed.Headers("x-index", fmt.Sprint(i)),
			}))(t)
			check(t, r == exspeed.PublishResult{Offset: uint64(i)}, "%+v", r)
		}
		all := must(c.Read(ctx, s, exspeed.ReadOptions{}))(t)
		var got []string
		for _, r := range all.Records {
			got = append(got, r.Subject)
		}
		check(t, fmt.Sprint(got) == fmt.Sprint(subjects), "%v", got)
		check(t, all.NextOffset == 5 && all.HighWatermark == 5, "%+v", all)
		first := all.Records[0]
		var body map[string]int
		noErr(t, first.JSON(&body))
		h, _ := first.Header("x-index")
		check(t, body["i"] == 0 && string(first.Key) == "k0" && h == "0", "%+v", first)
		check(t, time.Since(first.Time) < time.Minute, "time %v", first.Time)

		offsets := func(r *exspeed.ReadResult) string {
			var o []uint64
			for _, x := range r.Records {
				o = append(o, x.Offset)
			}
			return fmt.Sprint(o)
		}
		check(t, offsets(must(c.Read(ctx, s, exspeed.ReadOptions{Filter: "orders.*"}))(t)) == "[0 1 4]", "orders.*")
		check(t, offsets(must(c.Read(ctx, s, exspeed.ReadOptions{Filter: "orders.>"}))(t)) == "[0 1 2 4]", "orders.>")
		page := must(c.Read(ctx, s, exspeed.ReadOptions{From: 1, MaxRecords: 2}))(t)
		check(t, offsets(page) == "[1 2]" && page.NextOffset == 3, "%+v", page)
		_, err := c.Read(ctx, s, exspeed.ReadOptions{Filter: "orders.>.x"})
		check(t, code(t, err) == 400, "%v", err)
	})

	t.Run("long-poll read", func(t *testing.T) {
		s := newStream(t, c, "lp")
		start := time.Now()
		done := make(chan *exspeed.ReadResult, 1)
		go func() {
			r, err := c.Read(ctx, s, exspeed.ReadOptions{Wait: 5 * time.Second})
			if err != nil {
				t.Error(err)
			}
			done <- r
		}()
		time.Sleep(200 * time.Millisecond)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "late.arrival", Value: []byte("hello")}))(t)
		r := <-done
		check(t, r != nil && len(r.Records) == 1 && r.Records[0].Text() == "hello", "%+v", r)
		check(t, time.Since(start) < 4*time.Second, "took %v", time.Since(start))
	})

	t.Run("batches and dedup by msg id", func(t *testing.T) {
		s := newStream(t, c, "dedup")
		m1, m2, m3 := exspeed.NewMsgID(), exspeed.NewMsgID(), exspeed.NewMsgID()
		first := must(c.PublishBatch(ctx, s, []exspeed.PublishRecord{
			{Subject: "orders.placed", Value: []byte(`{"id":1}`), MsgID: m1},
			{Subject: "orders.placed", Value: []byte(`{"id":2}`), MsgID: m2},
		}))(t)
		check(t, fmt.Sprint(first) == "[{0 false} {1 false}]", "%v", first)
		retry := must(c.PublishBatch(ctx, s, []exspeed.PublishRecord{
			{Subject: "orders.placed", Value: []byte(`{"id":1}`), MsgID: m1},
			{Subject: "orders.placed", Value: []byte(`{"id":3}`), MsgID: m3},
		}))(t)
		check(t, fmt.Sprint(retry) == "[{0 true} {2 false}]", "%v", retry)
		r := must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "orders.placed", Value: []byte(`{"id":2}`), MsgID: m2}))(t)
		check(t, r == exspeed.PublishResult{Offset: 1, Duplicate: true}, "%+v", r)
		_, err := c.Publish(ctx, s, exspeed.PublishRecord{Subject: "orders.placed", Value: []byte(`{"id":99}`), MsgID: m1})
		check(t, code(t, err) == 409, "%v", err)
		off, ok := err.(*exspeed.ServerError).StoredOffset()
		check(t, ok && off == 0, "stored offset %d %v", off, ok)
		check(t, must(c.StreamInfo(ctx, s))(t).NextOffset == 3, "next offset")
	})

	t.Run("coalescing publisher keeps call order", func(t *testing.T) {
		s := newStream(t, c, "pub")
		p := c.NewPublisher(exspeed.PublisherOptions{MaxBatchRecords: 64})
		const n = 1000
		acks := make([]*exspeed.PubAck, n)
		for i := range acks {
			acks[i] = must(p.PublishAsync(ctx, s, exspeed.PublishRecord{Subject: "seq.value", Value: jsonValue(map[string]int{"i": i})}))(t)
		}
		for i, a := range acks {
			r := must(a.Wait(ctx))(t)
			check(t, r.Offset == uint64(i), "record %d at offset %d", i, r.Offset)
		}
		noErr(t, p.Close(ctx))
		var seen []int
		var from uint64
		for len(seen) < n {
			r := must(c.Read(ctx, s, exspeed.ReadOptions{From: from, MaxRecords: 500}))(t)
			for _, rec := range r.Records {
				var v map[string]int
				noErr(t, rec.JSON(&v))
				seen = append(seen, v["i"])
			}
			from = r.NextOffset
		}
		for i, v := range seen {
			check(t, v == i, "position %d holds %d", i, v)
		}
		// Concurrent publishers share one publisher; each gets its own result.
		p2 := c.NewPublisher(exspeed.PublisherOptions{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		offsets := map[uint64]bool{}
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					r, err := p2.Publish(ctx, s, exspeed.PublishRecord{Subject: "seq.more"})
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					offsets[r.Offset] = true
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		check(t, len(offsets) == 400, "distinct offsets %d", len(offsets))
		noErr(t, p2.Close(ctx))
		_, err := p2.Publish(ctx, s, exspeed.PublishRecord{Subject: "seq.more"})
		check(t, errors.Is(err, exspeed.ErrPublisherClosed), "%v", err)
		check(t, must(c.StreamInfo(ctx, s))(t).NextOffset == n+400, "next offset")
	})
}

func TestE2EConsumers(t *testing.T) {
	srv := shared(t)
	exspeed.CheckLeaksForTest(t)
	c := srv.connect(t)
	ctx := ctxFor(t, 90*time.Second)

	t.Run("subscribe, receive and ack", func(t *testing.T) {
		s := newStream(t, c, "s")
		cons := uniq("billing")
		info := must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, FilterSubjects: []string{"work.>"}}))(t)
		check(t, info.Spec.Name == cons && info.Spec.Stream == s && fmt.Sprint(info.Spec.FilterSubjects) == "[work.>]" &&
			info.Spec.Deliver.String() == "all" && info.Spec.Ack == exspeed.AckExplicit, "%+v", info.Spec)
		// Idempotent for the same spec (also when passed back from info); 409 for a different one.
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, FilterSubjects: []string{"work.>"}}))(t)
		must(c.CreateConsumer(ctx, info.Spec))(t)
		_, err := c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s})
		check(t, code(t, err) == 409, "%v", err)
		list := must(c.ListConsumers(ctx, s))(t)
		check(t, len(list) == 1 && list[0].Spec.Name == cons, "%+v", list)

		publishN(t, c, s, 3, "work.item")
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "other.thing", Value: []byte("filtered out")}))(t)
		sub := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 10}))(t)
		var got []string
		for m := range sub.Messages() {
			var v map[string]int
			noErr(t, m.JSON(&v))
			got = append(got, fmt.Sprintf("%d/%d/%d", m.Offset, m.DeliveryCount, v["i"]))
			m.Ack()
			if len(got) == 3 {
				noErr(t, sub.Unsubscribe(ctx))
			}
		}
		check(t, fmt.Sprint(got) == "[0/1/0 1/1/1 2/1/2]", "%v", got)
		check(t, sub.EndReason().Code == 0, "%v", sub.EndReason())
		eventually(t, 10*time.Second, func() bool {
			i, err := c.ConsumerInfo(ctx, cons)
			return err == nil && i.NumUnacked == 0 && i.Stats.Acked == 3 && i.AckFloor >= 3
		})
	})

	t.Run("credit window", func(t *testing.T) {
		s := newStream(t, c, "credit")
		cons := uniq("credit")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		publishN(t, c, s, 50, "work.item")
		sub := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 4}))(t)
		eventually(t, 10*time.Second, func() bool { return sub.Buffered() == 4 })
		time.Sleep(300 * time.Millisecond)
		check(t, sub.Buffered() == 4, "pushed beyond the window: %d", sub.Buffered())
		for i := 0; i < 50; i++ {
			m := nextMsg(t, sub, 5*time.Second)
			check(t, m.Offset == uint64(i), "offset %d at %d", m.Offset, i)
			check(t, sub.Buffered() <= 4, "buffered %d", sub.Buffered())
			m.Ack()
		}
		noErr(t, sub.Unsubscribe(ctx))
	})

	t.Run("nack redelivers with a higher delivery count", func(t *testing.T) {
		s := newStream(t, c, "nack")
		cons := uniq("nack")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.item", Value: []byte("retry me")}))(t)
		sub := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 10}))(t)
		first := nextMsg(t, sub, 5*time.Second)
		check(t, first.DeliveryCount == 1, "count %d", first.DeliveryCount)
		noErr(t, first.Nack(ctx, 0))
		second := nextMsg(t, sub, 5*time.Second)
		check(t, second.Offset == first.Offset && second.DeliveryCount == 2, "%+v", second)
		noErr(t, second.Nack(ctx, 200*time.Millisecond))
		start := time.Now()
		third := nextMsg(t, sub, 5*time.Second)
		check(t, third.DeliveryCount == 3 && time.Since(start) >= 150*time.Millisecond, "%d after %v", third.DeliveryCount, time.Since(start))
		noErr(t, c.Ack(ctx, cons, third.Offset))
		check(t, must(c.ConsumerInfo(ctx, cons))(t).NumUnacked == 0, "unacked")
		noErr(t, sub.Unsubscribe(ctx))
	})

	t.Run("two clients share one consumer", func(t *testing.T) {
		s := newStream(t, c, "shared")
		cons := uniq("shared")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		other := srv.connect(t, exspeed.WithClientID("e2e-2"))
		subA := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 8}))(t)
		subB := must(other.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 8}))(t)
		var mu sync.Mutex
		seen := map[string][]uint64{}
		total := 0
		var wg sync.WaitGroup
		drain := func(name string, sub *exspeed.Subscription) {
			defer wg.Done()
			for m := range sub.Messages() {
				if m.DeliveryCount != 1 {
					t.Errorf("redelivery %d of %d", m.DeliveryCount, m.Offset)
				}
				mu.Lock()
				seen[name] = append(seen[name], m.Offset)
				total++
				mu.Unlock()
				m.Ack()
				time.Sleep(time.Millisecond) // let the other subscriber get a share
			}
		}
		wg.Add(2)
		go drain("a", subA)
		go drain("b", subB)
		publishN(t, c, s, 200, "work.item")
		eventually(t, 15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return total >= 200 })
		time.Sleep(200 * time.Millisecond) // anything extra would show up now
		noErr(t, subA.Unsubscribe(ctx))
		noErr(t, subB.Unsubscribe(ctx))
		wg.Wait()
		all := append(append([]uint64(nil), seen["a"]...), seen["b"]...)
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		check(t, len(seen["a"]) > 0 && len(seen["b"]) > 0, "a=%d b=%d", len(seen["a"]), len(seen["b"]))
		check(t, len(all) == 200, "got %d records", len(all))
		for i, o := range all {
			check(t, o == uint64(i), "offset %d at %d", o, i)
		}
	})

	t.Run("dead-letters after max deliver and on term", func(t *testing.T) {
		s := newStream(t, c, "dl")
		dlq := newStream(t, c, "dlq")
		cons := uniq("dlq-c")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, MaxDeliver: 2, DLQStream: dlq}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.poison", Value: []byte("bad")}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.terminal", Value: []byte("worse")}))(t)
		pull1 := func() *exspeed.Message {
			msgs := must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 1, Expires: 2 * time.Second}))(t)
			check(t, len(msgs) == 1, "pulled %d", len(msgs))
			return msgs[0]
		}
		m1 := pull1()
		check(t, m1.DeliveryCount == 1, "count")
		noErr(t, m1.Nack(ctx, 0))
		m2 := pull1()
		check(t, m2.Offset == 0 && m2.DeliveryCount == 2, "%+v", m2)
		noErr(t, m2.Nack(ctx, 0))
		term := pull1()
		check(t, term.Offset == 1, "offset %d", term.Offset)
		noErr(t, term.Term(ctx, "cannot parse"))

		var dead []exspeed.Record
		eventually(t, 10*time.Second, func() bool {
			r, err := c.Read(ctx, dlq, exspeed.ReadOptions{})
			if err == nil && len(r.Records) == 2 {
				dead = r.Records
				return true
			}
			return false
		})
		hdr := func(r exspeed.Record, k string) string { v, _ := r.Header(k); return v }
		check(t, dead[0].Text() == "bad" && dead[1].Text() == "worse", "dead letters")
		check(t, hdr(dead[0], "exspeed-dlq-origin") == cons && hdr(dead[0], "exspeed-dlq-stream") == s, "origin")
		check(t, hdr(dead[0], "exspeed-dlq-original-offset") == "0" && hdr(dead[0], "exspeed-dlq-deliveries") == "2", "dlq headers")
		check(t, hdr(dead[1], "exspeed-dlq-original-offset") == "1" && strings.Contains(hdr(dead[1], "exspeed-dlq-reason"), "cannot parse"), "term headers")
		check(t, must(c.ConsumerInfo(ctx, cons))(t).Stats.DeadLettered == 2, "dead lettered")
		check(t, len(must(c.Pull(ctx, cons, exspeed.PullOptions{Expires: 300 * time.Millisecond}))(t)) == 0, "nothing left")
	})

	t.Run("pull long-polls and times out empty", func(t *testing.T) {
		s := newStream(t, c, "pull")
		cons := uniq("pull")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		start := time.Now()
		check(t, len(must(c.Pull(ctx, cons, exspeed.PullOptions{Expires: 300 * time.Millisecond}))(t)) == 0, "empty")
		check(t, time.Since(start) >= 250*time.Millisecond, "returned after %v", time.Since(start))

		start = time.Now()
		done := make(chan []*exspeed.Message, 1)
		go func() {
			msgs, err := c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 10, Expires: 10 * time.Second})
			if err != nil {
				t.Error(err)
			}
			done <- msgs
		}()
		time.Sleep(200 * time.Millisecond)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.item", Value: []byte("now")}))(t)
		msgs := <-done
		check(t, len(msgs) == 1 && msgs[0].Text() == "now" && time.Since(start) < 5*time.Second, "%v", msgs)
		// A long pull doesn't block other requests on the same connection.
		slow := make(chan struct{})
		go func() {
			_, _ = c.Pull(ctx, cons, exspeed.PullOptions{Expires: time.Second})
			close(slow)
		}()
		time.Sleep(50 * time.Millisecond)
		check(t, must(c.Ping(ctx))(t) < 500*time.Millisecond, "ping blocked")
		<-slow
		msgs[0].Ack()
	})

	t.Run("seek", func(t *testing.T) {
		s := newStream(t, c, "seek")
		cons := uniq("seek")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, Ack: exspeed.AckNone}))(t)
		publishN(t, c, s, 5, "work.item")
		offsets := func() string {
			var o []uint64
			for _, m := range must(c.Pull(ctx, cons, exspeed.PullOptions{MaxMessages: 100, Expires: 300 * time.Millisecond}))(t) {
				o = append(o, m.Offset)
			}
			return fmt.Sprint(o)
		}
		check(t, offsets() == "[0 1 2 3 4]", "start")
		noErr(t, c.Seek(ctx, cons, exspeed.SeekOffset(2)))
		check(t, offsets() == "[2 3 4]", "offset")
		noErr(t, c.Seek(ctx, cons, exspeed.SeekEarliest()))
		check(t, offsets() == "[0 1 2 3 4]", "earliest")
		noErr(t, c.Seek(ctx, cons, exspeed.SeekLatest()))
		check(t, offsets() == "[]", "latest")
		noErr(t, c.Seek(ctx, cons, exspeed.SeekTime(time.UnixMilli(0))))
		check(t, offsets() == "[0 1 2 3 4]", "time 0")
		noErr(t, c.Seek(ctx, cons, exspeed.SeekTime(time.Now().Add(time.Minute))))
		check(t, offsets() == "[]", "future")
		check(t, code(t, c.Seek(ctx, uniq("nobody"), exspeed.SeekEarliest())) == 404, "unknown")
	})

	t.Run("ephemeral consumer removed when its connection closes", func(t *testing.T) {
		s := newStream(t, c, "eph")
		cons := uniq("eph")
		owner := must(srv.tryConnect())(t)
		must(owner.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s, Ephemeral: true, Deliver: exspeed.DeliverNew()}))(t)
		check(t, must(c.ConsumerInfo(ctx, cons))(t).Spec.Ephemeral, "ephemeral")
		noErr(t, owner.Close())
		eventually(t, 10*time.Second, func() bool {
			_, err := c.ConsumerInfo(ctx, cons)
			return errors.Is(err, exspeed.ErrNotFound)
		})
	})

	t.Run("subscription ends with 404 when the consumer is deleted", func(t *testing.T) {
		s := newStream(t, c, "gone")
		cons := uniq("gone")
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		sub := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{}))(t)
		done := make(chan error, 1)
		go func() {
			_, err := sub.Next(ctx)
			done <- err
		}()
		noErr(t, c.DeleteConsumer(ctx, cons))
		err := <-done
		check(t, errors.Is(err, exspeed.ErrSubscriptionEnded) && sub.EndReason().Code == 404, "%v", err)
	})
}

func TestE2EAuth(t *testing.T) {
	srv := startServer(t, serverOpts{authToken: "s3cret-token"})
	exspeed.CheckLeaksForTest(t)
	ctx := ctxFor(t, 30*time.Second)
	for _, opts := range [][]exspeed.Option{{exspeed.WithToken("wrong")}, nil} {
		_, err := srv.tryConnect(opts...)
		check(t, code(t, err) == 401 && errors.Is(err, exspeed.ErrUnauthorized), "%v", err)
	}
	c := srv.connect(t, exspeed.WithToken("s3cret-token"))
	s := uniq("authed")
	noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s}))
	check(t, must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "auth.ok", Value: []byte("yes")}))(t).Offset == 0, "offset")
	r := must(c.Query(ctx, fmt.Sprintf(`SELECT COUNT(*) AS n FROM "%s"`, s)))(t)
	check(t, fmt.Sprint(r.Rows) == "[[1]]", "%v", r.Rows)
}

func TestE2EReconnection(t *testing.T) {
	srv := startServer(t, serverOpts{})
	exspeed.CheckLeaksForTest(t)
	ctx := ctxFor(t, 60*time.Second)

	t.Run("re-subscribes after a restart; unacked records are redelivered", func(t *testing.T) {
		disconnected := make(chan struct{}, 4)
		reconnected := make(chan struct{}, 4)
		c := srv.connect(t,
			exspeed.WithReconnect(exspeed.ReconnectPolicy{InitialDelay: 50 * time.Millisecond, MaxDelay: 200 * time.Millisecond}),
			exspeed.WithDisconnectHandler(func(error) { disconnected <- struct{}{} }),
			exspeed.WithReconnectHandler(func(exspeed.ServerInfo) { reconnected <- struct{}{} }))
		s, cons := uniq("durable"), uniq("durable-c")
		noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s}))
		must(c.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: cons, Stream: s}))(t)
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.item", Value: []byte("before")}))(t)
		sub := must(c.Subscribe(ctx, cons, exspeed.SubscribeOptions{Window: 10}))(t)
		check(t, nextMsg(t, sub, 5*time.Second).Text() == "before", "first") // not acked

		srv.restart(t)
		select {
		case <-reconnected:
		case <-time.After(20 * time.Second):
			t.Fatal("no reconnect")
		}
		check(t, len(disconnected) == 1, "disconnect events %d", len(disconnected))
		check(t, c.Connected(), "connected")
		again := nextMsg(t, sub, 10*time.Second)
		check(t, again.Text() == "before", "redelivered %q", again.Text())
		// (The delivery count may restart at 1: the server persists consumer
		// state in periodic snapshots.)
		again.Ack()
		must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "work.item", Value: []byte("after")}))(t)
		m2 := nextMsg(t, sub, 5*time.Second)
		check(t, m2.Text() == "after", "after")
		m2.Ack()
		check(t, !sub.Closed(), "closed")
	})

	t.Run("requests fail with ConnectionError when reconnection is off", func(t *testing.T) {
		closed := make(chan struct{})
		c := srv.connect(t, exspeed.WithCloseHandler(func(error) { close(closed) }))
		srv.restart(t)
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Fatal("no close")
		}
		_, err := c.Ping(ctx)
		var ce *exspeed.ConnectionError
		check(t, errors.As(err, &ce), "%v", err)
	})
}

func TestE2ETLS(t *testing.T) {
	if serverBin() == "" {
		t.Skip(skipMessage())
	}
	cs := makeCerts(t)
	srv := startServer(t, serverOpts{tlsCert: cs.serverCert, tlsKey: cs.serverKey})
	exspeed.CheckLeaksForTest(t)
	ctx := ctxFor(t, 30*time.Second)

	cfg := must(exspeed.LoadTLSConfig(cs.caFile, "", ""))(t)
	c := srv.connect(t, exspeed.WithTLS(cfg))
	s := uniq("tls")
	noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s}))
	check(t, must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "tls.ok", Value: []byte("secure")}))(t).Offset == 0, "offset")

	// An untrusted certificate, and plain TCP, are refused.
	var ce *exspeed.ConnectionError
	_, err := srv.tryConnect(exspeed.WithTLS(nil), exspeed.WithRequestTimeout(3*time.Second))
	check(t, errors.As(err, &ce), "untrusted: %v", err)
	_, err = srv.tryConnect(exspeed.WithRequestTimeout(3 * time.Second))
	check(t, errors.As(err, &ce), "plain: %v", err)
}

func TestE2EMutualTLS(t *testing.T) {
	if serverBin() == "" {
		t.Skip(skipMessage())
	}
	cs := makeCerts(t)
	srv := startServer(t, serverOpts{tlsCert: cs.serverCert, tlsKey: cs.serverKey, tlsClientCA: cs.caFile})
	exspeed.CheckLeaksForTest(t)
	ctx := ctxFor(t, 30*time.Second)

	cfg := must(exspeed.LoadTLSConfig(cs.caFile, cs.clientCert, cs.clientKey))(t)
	c := srv.connect(t, exspeed.WithTLS(cfg))
	s := uniq("mtls")
	noErr(t, c.CreateStream(ctx, exspeed.StreamSpec{Name: s}))
	check(t, must(c.Publish(ctx, s, exspeed.PublishRecord{Subject: "mtls.ok", Value: []byte("mutual")}))(t).Offset == 0, "offset")

	noCert := must(exspeed.LoadTLSConfig(cs.caFile, "", ""))(t)
	_, err := srv.tryConnect(exspeed.WithTLS(noCert), exspeed.WithRequestTimeout(3*time.Second))
	var ce *exspeed.ConnectionError
	check(t, errors.As(err, &ce), "without a client certificate: %v", err)
}
