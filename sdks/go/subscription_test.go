package exspeed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// subscribeOk answers Subscribe with id, followed by pushes in the same write.
func subscribeOk(id uint32, pushes ...proto.Message) handlerFunc {
	return func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, ok := req.(proto.Subscribe); !ok {
			return false
		}
		frames := []frame{{corr, proto.SubscribeOk{SubID: id}}}
		for _, p := range pushes {
			frames = append(frames, frame{0, p})
		}
		fc.replyMany(frames...)
		return true
	}
}

func next(t *testing.T, s *Subscription) *Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDeliverInTheSameChunkAsSubscribeOkIsKept(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(subscribeOk(5, proto.Deliver{SubID: 5, Records: []proto.WireRecord{wrec(0, "v0"), wrec(1, "v1")}}))
	sub, err := c.Subscribe(testCtx(t), "billing", SubscribeOptions{Window: 10})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, sub.ID(), uint32(5))
	m0, m1 := next(t, sub), next(t, sub)
	eq(t, []uint64{m0.Offset, m1.Offset}, []uint64{0, 1})
	eq(t, m0.Text(), "v0")
	if v, ok := m0.Header("h"); !ok || v != "1" {
		t.Fatal(v, ok)
	}
	eq(t, m0.DeliveryCount, 1)
	eq(t, m0.Consumer, "billing")
	eq(t, m0.Time, time.Unix(0, 1_700_000_000_000_000_000))
	eq(t, s.last().of(proto.Subscribe{})[0].req, proto.Message(proto.Subscribe{Consumer: "billing", Credits: 10}))
}

func TestCreditIsReturnedInBatchesOfHalfTheWindow(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(subscribeOk(9))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{Window: 4})
	if err != nil {
		t.Fatal(err)
	}
	if s.last().of(proto.Subscribe{})[0].corr == 0 {
		t.Fatal("subscribe needs a correlation id")
	}
	s.last().reply(0, proto.Deliver{SubID: 9, Records: []proto.WireRecord{wrec(0, ""), wrec(1, ""), wrec(2, ""), wrec(3, "")}})
	next(t, sub)
	time.Sleep(20 * time.Millisecond)
	eq(t, len(s.last().of(proto.Credit{})), 0)
	next(t, sub)
	s.until(func() bool { return len(s.last().of(proto.Credit{})) == 1 })
	cr := s.last().of(proto.Credit{})[0]
	eq(t, cr, received{corr: 0, req: proto.Credit{SubID: 9, Credits: 2}})
	next(t, sub)
	next(t, sub)
	s.until(func() bool { return len(s.last().of(proto.Credit{})) == 2 })
}

func TestMessagesSettleWithAckNackTermInProgress(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	sub := subscribeOk(1, proto.Deliver{SubID: 1, Records: []proto.WireRecord{wrec(7, "")}})
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Nack, proto.Term, proto.InProgress, proto.Ack:
			fc.reply(corr, proto.Ok{})
			return true
		}
		return sub(fc, corr, req)
	})
	ss, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	m := next(t, ss)
	ctx := testCtx(t)
	m.Ack()
	if err := m.Nack(ctx, 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := m.Term(ctx, "poison"); err != nil {
		t.Fatal(err)
	}
	if err := m.InProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.AckSync(ctx); err != nil {
		t.Fatal(err)
	}
	var got []received
	for _, r := range s.last().received()[2:] {
		got = append(got, r)
	}
	eq(t, len(got), 5)
	eq(t, got[0], received{0, proto.Ack{Consumer: "c", Offsets: []uint64{7}}})
	eq(t, got[1].req, proto.Message(proto.Nack{Consumer: "c", Offset: 7, DelayMs: 250}))
	eq(t, got[2].req, proto.Message(proto.Term{Consumer: "c", Offset: 7, Reason: "poison"}))
	eq(t, got[3].req, proto.Message(proto.InProgress{Consumer: "c", Offsets: []uint64{7}}))
	eq(t, got[4].req, proto.Message(proto.Ack{Consumer: "c", Offsets: []uint64{7}}))
	for _, r := range got[1:] {
		if r.corr == 0 {
			t.Fatalf("%T must be a request", r.req)
		}
	}
}

func TestAcksShareOneFrameAndPrecedeLaterRequests(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	c.cfg.ackLinger = time.Hour // only the Ping flushes them
	s.setHandler(subscribeOk(1, proto.Deliver{SubID: 1, Records: []proto.WireRecord{wrec(1, ""), wrec(2, ""), wrec(3, "")}}))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	msgs := []*Message{next(t, sub), next(t, sub), next(t, sub)}
	for _, m := range msgs {
		m.Ack()
	}
	if _, err := c.Ping(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	// (window 256: no Credit is due after three messages)
	eq(t, s.last().types(), []string{"Subscribe", "Ack", "Ping"})
	eq(t, s.last().of(proto.Ack{})[0], received{0, proto.Ack{Consumer: "c", Offsets: []uint64{1, 2, 3}}})
}

func TestAcksAreFlushedWhenTheBufferRunsDry(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	c.cfg.ackLinger = time.Hour
	s.setHandler(subscribeOk(1, proto.Deliver{SubID: 1, Records: []proto.WireRecord{wrec(1, ""), wrec(2, "")}}))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub).Ack()
	next(t, sub).Ack()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	s.until(func() bool { return len(s.last().of(proto.Ack{})) == 1 })
	eq(t, s.last().of(proto.Ack{})[0].req, proto.Message(proto.Ack{Consumer: "c", Offsets: []uint64{1, 2}}))

}

func TestAckLingerFlushesOnItsOwn(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	c.cfg.ackLinger = 10 * time.Millisecond
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, ok := req.(proto.Pull); ok {
			fc.reply(corr, proto.Messages{Records: []proto.WireRecord{wrec(4, "")}})
			return true
		}
		return false
	})
	msgs, err := c.Pull(testCtx(t), "c", PullOptions{})
	if err != nil {
		t.Fatal(err)
	}
	msgs[0].Ack()
	s.until(func() bool { return len(s.last().of(proto.Ack{})) == 1 })
}

func TestQueuedAcksAreFlushedOnClose(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	c.cfg.ackLinger = time.Hour
	s.setHandler(subscribeOk(1, proto.Deliver{SubID: 1, Records: []proto.WireRecord{wrec(4, "")}}))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub).Ack()
	fc := s.last()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	s.until(func() bool { return len(fc.of(proto.Ack{})) == 1 })
	var ended *SubscriptionEndedError
	if _, err := sub.Next(testCtx(t)); !errors.As(err, &ended) || ended.Code != 0 {
		t.Fatal(err)
	}
}

func TestAsyncErrorsGoToTheErrorHandler(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	errs := make(chan error, 1)
	connectFake(t, s, WithErrorHandler(func(err error) { errs <- err }))
	s.last().reply(0, proto.Error{Code: 404, Message: "consumer 'x' not found"})
	select {
	case err := <-errs:
		mustCode(t, err, 404)
	case <-time.After(5 * time.Second):
		t.Fatal("no async error")
	}
}

func TestSubscriptionEndedAfterBufferedRecords(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	s.setHandler(subscribeOk(3,
		proto.Deliver{SubID: 3, Records: []proto.WireRecord{wrec(0, "")}},
		proto.SubscriptionEnded{SubID: 3, Code: 404, Message: "consumer deleted"}))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var seen []uint64
	for m := range sub.Messages() {
		seen = append(seen, m.Offset)
	}
	eq(t, seen, []uint64{0})
	eq(t, sub.EndReason(), &SubscriptionEndedError{Code: 404, Message: "consumer deleted"})
	_, err = sub.Next(testCtx(t))
	if !errors.Is(err, ErrSubscriptionEnded) {
		t.Fatal(err)
	}
}

func TestUnsubscribe(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s)
	sub := subscribeOk(4, proto.Deliver{SubID: 4, Records: []proto.WireRecord{wrec(0, ""), wrec(1, "")}})
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, ok := req.(proto.Unsubscribe); ok {
			fc.reply(corr, proto.Ok{})
			return true
		}
		return sub(fc, corr, req)
	})
	ss, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ch := ss.Messages()
	<-ch
	if err := ss.Unsubscribe(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	eq(t, s.last().of(proto.Unsubscribe{})[0].req, proto.Message(proto.Unsubscribe{SubID: 4}))
	eq(t, ss.EndReason(), &SubscriptionEndedError{Code: 0, Message: "unsubscribed"})
	for range ch { // closed after the unsubscribe
	}
	if _, err := ss.Next(testCtx(t)); !errors.Is(err, ErrSubscriptionEnded) {
		t.Fatal(err)
	}
	if !ss.Closed() {
		t.Fatal("closed")
	}
}

func TestSubscriptionsEndWith503WhenTheConnectionIsLostAndReconnectIsOff(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	closed := make(chan error, 1)
	c := connectFake(t, s, WithCloseHandler(func(err error) { closed <- err }))
	s.setHandler(subscribeOk(1))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := sub.Next(context.Background())
		done <- err
	}()
	s.last().drop()
	err = <-done
	var ended *SubscriptionEndedError
	if !errors.As(err, &ended) || ended.Code != 503 {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		var ce *ConnectionError
		if !errors.As(err, &ce) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no close event")
	}
	if c.Connected() {
		t.Fatal("connected")
	}
}

func TestReconnectRecreatesEphemeralConsumersAndResubscribes(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var mu sync.Mutex
	nextSub := uint32(1)
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		switch req.(type) {
		case proto.Subscribe:
			mu.Lock()
			id := nextSub
			nextSub++
			mu.Unlock()
			fc.reply(corr, proto.SubscribeOk{SubID: id})
		case proto.CreateConsumer:
			fc.reply(corr, proto.JSON{Data: []byte(`{}`)})
		default:
			return false
		}
		return true
	})
	var events []string
	var emu sync.Mutex
	reconnected := make(chan struct{}, 1)
	c := connectFake(t, s,
		WithReconnect(ReconnectPolicy{InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}),
		WithDisconnectHandler(func(error) { emu.Lock(); events = append(events, "disconnect"); emu.Unlock() }),
		WithReconnectHandler(func(ServerInfo) {
			emu.Lock()
			events = append(events, "reconnect")
			emu.Unlock()
			reconnected <- struct{}{}
		}))
	ctx := testCtx(t)
	if _, err := c.CreateConsumer(ctx, ConsumerSpec{Name: "tmp", Stream: "s", Ephemeral: true}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, "tmp", SubscribeOptions{Window: 8})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, sub.ID(), uint32(1))
	s.last().reply(0, proto.Deliver{SubID: 1, Records: []proto.WireRecord{wrec(0, "")}})
	eq(t, next(t, sub).Offset, uint64(0))

	s.connections()[0].drop()
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("no reconnect")
	}
	emu.Lock()
	eq(t, events, []string{"disconnect", "reconnect"})
	emu.Unlock()
	conns := s.connections()
	eq(t, len(conns), 2)
	second := conns[1]
	eq(t, second.types(), []string{"CreateConsumer", "Subscribe"})
	eq(t, second.of(proto.Subscribe{})[0].req, proto.Message(proto.Subscribe{Consumer: "tmp", Credits: 8}))
	eq(t, sub.ID(), uint32(2))
	second.reply(0, proto.Deliver{SubID: 2, Records: []proto.WireRecord{wrec(1, "")}})
	eq(t, next(t, sub).Offset, uint64(1))
	if !c.Connected() {
		t.Fatal("not connected")
	}
}

func TestAFailedResubscribeEndsTheSubscription(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	var mu sync.Mutex
	first := true
	s.setHandler(func(fc *fakeConn, corr uint32, req proto.Message) bool {
		if _, ok := req.(proto.Subscribe); !ok {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		if first {
			fc.reply(corr, proto.SubscribeOk{SubID: 1})
		} else {
			fc.reply(corr, proto.Error{Code: 404, Message: "consumer 'c' not found"})
		}
		first = false
		return true
	})
	reconnected := make(chan struct{}, 1)
	c := connectFake(t, s, WithReconnect(ReconnectPolicy{InitialDelay: 10 * time.Millisecond}),
		WithReconnectHandler(func(ServerInfo) { reconnected <- struct{}{} }))
	sub, err := c.Subscribe(testCtx(t), "c", SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.connections()[0].drop()
	<-reconnected
	_, err = sub.Next(testCtx(t))
	if !errors.Is(err, ErrSubscriptionEnded) {
		t.Fatal(err)
	}
	eq(t, sub.EndReason(), &SubscriptionEndedError{Code: 404, Message: "consumer 'c' not found"})
}

func TestReconnectGivesUpAfterMaxAttempts(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	closed := make(chan error, 1)
	c := connectFake(t, s, WithReconnect(ReconnectPolicy{MaxAttempts: 2, InitialDelay: 10 * time.Millisecond}),
		WithCloseHandler(func(err error) { closed <- err }))
	s.close()
	select {
	case err := <-closed:
		var ce *ConnectionError
		if !errors.As(err, &ce) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no close")
	}
	if c.Connected() {
		t.Fatal("connected")
	}
	if _, err := c.Ping(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestRequestsFailWhileReconnecting(t *testing.T) {
	checkLeaks(t)
	s := startFake(t)
	c := connectFake(t, s, WithReconnect(ReconnectPolicy{InitialDelay: time.Hour}))
	s.last().drop()
	waitFor(t, func() bool { return !c.Connected() })
	_, err := c.Ping(testCtx(t))
	var ce *ConnectionError
	if !errors.As(err, &ce) || errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
