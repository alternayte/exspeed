package exspeed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// PublisherOptions configure [Client.NewPublisher]. Zero values are the
// defaults.
type PublisherOptions struct {
	// BatchWindow is how long to gather records before sending a batch.
	// 0 (default) sends whatever has been queued as soon as the publisher's
	// goroutine runs, without adding latency.
	BatchWindow time.Duration
	// MaxBatchRecords is the most records per request. Default 512.
	MaxBatchRecords int
	// MaxInFlight is the most records accepted but not yet acknowledged;
	// Publish waits while it is reached. Default 4096.
	MaxInFlight int
}

// maxBatchBytes keeps each batch frame well below the 16 MiB frame limit.
const maxBatchBytes = 4 * 1024 * 1024

// PubAck is the pending result of [Publisher.PublishAsync].
type PubAck struct {
	done chan struct{}
	res  PublishResult
	err  error
}

// Done is closed once the result is known.
func (a *PubAck) Done() <-chan struct{} { return a.done }

// Result waits for the result.
func (a *PubAck) Result() (PublishResult, error) {
	<-a.done
	return a.res, a.err
}

// Wait waits for the result as long as ctx allows.
func (a *PubAck) Wait(ctx context.Context) (PublishResult, error) {
	select {
	case <-a.done:
		return a.res, a.err
	case <-ctx.Done():
		return PublishResult{}, ctx.Err()
	}
}

func (a *PubAck) resolve(res PublishResult, err error) {
	a.res, a.err = res, err
	close(a.done)
}

type pubItem struct {
	stream string
	record proto.PublishRecord
	size   int
	ack    *PubAck
}

// Publisher is a pipelined, coalescing publisher. Concurrent publishes are
// gathered into PublishBatch requests (one per run of records for the same
// stream), and many batches can be in flight at once. Records reach each
// stream in the order Publish / PublishAsync was called, and every call
// gets its own record's result.
//
// A Publisher is safe for concurrent use. Its goroutine runs only while
// records are queued, so an idle publisher holds no resources; call Close
// (or Flush) to wait for everything accepted.
type Publisher struct {
	c        *Client
	window   time.Duration
	maxBatch int
	sem      chan struct{}
	mu       sync.Mutex
	queue    []*pubItem
	flushing bool
	flushNow bool
	wake     chan struct{}
	inFlight int
	idle     chan struct{}
	closed   bool
}

// NewPublisher returns a coalescing publisher on this client; see
// [Publisher].
func (c *Client) NewPublisher(opts PublisherOptions) *Publisher {
	if opts.MaxBatchRecords <= 0 {
		opts.MaxBatchRecords = 512
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = 4096
	}
	if opts.BatchWindow < 0 {
		opts.BatchWindow = 0
	}
	return &Publisher{
		c:        c,
		window:   opts.BatchWindow,
		maxBatch: opts.MaxBatchRecords,
		sem:      make(chan struct{}, opts.MaxInFlight),
		wake:     make(chan struct{}, 1),
	}
}

// Pending is the number of records accepted and not yet acknowledged.
func (p *Publisher) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inFlight
}

// Publish publishes one record and waits for its result.
func (p *Publisher) Publish(ctx context.Context, stream string, rec PublishRecord) (PublishResult, error) {
	a, err := p.PublishAsync(ctx, stream, rec)
	if err != nil {
		return PublishResult{}, err
	}
	return a.Wait(ctx)
}

// PublishAsync queues one record and returns without waiting for the
// server. It blocks (as long as ctx allows) only while MaxInFlight records
// are outstanding. Records queued by one goroutine reach the stream in
// call order.
func (p *Publisher) PublishAsync(ctx context.Context, stream string, rec PublishRecord) (*PubAck, error) {
	w, err := rec.toWire()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, ErrPublisherClosed
	}
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	size := 64 + len(w.Subject) + len(w.Value) + len(w.Key)
	if w.MsgID != nil {
		size += len(*w.MsgID)
	}
	for _, h := range w.Headers {
		size += 4 + len(h.Key) + len(h.Value)
	}
	item := &pubItem{stream: stream, record: w, size: size, ack: &PubAck{done: make(chan struct{})}}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.sem
		return nil, ErrPublisherClosed
	}
	p.queue = append(p.queue, item)
	p.inFlight++
	if !p.flushing {
		p.flushing = true
		go p.flushLoop()
	} else if len(p.queue) >= p.maxBatch {
		p.wakeLocked()
	}
	p.mu.Unlock()
	return item.ack, nil
}

// wakeLocked cuts the batch window short. Call with p.mu held.
func (p *Publisher) wakeLocked() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Flush sends queued records at once (without waiting for the batch
// window) and waits until every accepted record has been acknowledged (or
// failed), as long as ctx allows.
func (p *Publisher) Flush(ctx context.Context) error {
	p.mu.Lock()
	if len(p.queue) > 0 {
		p.flushNow = true
		p.wakeLocked()
	}
	if p.inFlight == 0 {
		p.mu.Unlock()
		return nil
	}
	if p.idle == nil {
		p.idle = make(chan struct{})
	}
	idle := p.idle
	p.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close rejects further publishes with [ErrPublisherClosed], then waits
// for everything accepted (see Flush).
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return p.Flush(ctx)
}

// flushLoop sends queued records until the queue is empty, then exits.
// Only one runs at a time, and it writes each request before taking the
// next run, so wire order matches queue order.
func (p *Publisher) flushLoop() {
	for {
		p.mu.Lock()
		if len(p.queue) == 0 {
			p.flushing = false
			p.mu.Unlock()
			return
		}
		now := len(p.queue) >= p.maxBatch || p.flushNow
		p.mu.Unlock()
		if p.window > 0 && !now {
			t := time.NewTimer(p.window)
			select {
			case <-t.C:
			case <-p.wake:
				t.Stop()
			}
		}
		p.mu.Lock()
		queue := p.queue
		p.queue = nil
		p.flushNow = false
		select { // a stale wake-up would cut the next window short
		case <-p.wake:
		default:
		}
		p.mu.Unlock()
		for len(queue) > 0 {
			stream := queue[0].stream
			n, bytes := 0, 0
			for n < len(queue) && queue[n].stream == stream && n < p.maxBatch &&
				(n == 0 || bytes+queue[n].size <= maxBatchBytes) {
				bytes += queue[n].size
				n++
			}
			p.sendRun(stream, queue[:n])
			queue = queue[n:]
		}
	}
}

func (p *Publisher) sendRun(stream string, run []*pubItem) {
	var req proto.Message
	if len(run) == 1 {
		req = proto.Publish{Stream: stream, Record: run[0].record}
	} else {
		recs := make([]proto.PublishRecord, len(run))
		for i, it := range run {
			recs[i] = it.record
		}
		req = proto.PublishBatch{Stream: stream, Records: recs}
	}
	err := p.c.requestAsync(req, func(resp proto.Message, err error) {
		if err == nil {
			switch r := resp.(type) {
			case proto.PublishOk:
				if len(run) == 1 {
					run[0].ack.resolve(PublishResult{Offset: r.Offset, Duplicate: r.Duplicate}, nil)
					p.release(1)
					return
				}
			case proto.PublishBatchOk:
				if len(r.Results) == len(run) {
					for i, x := range r.Results {
						run[i].ack.resolve(PublishResult{Offset: x.Offset, Duplicate: x.Duplicate}, nil)
					}
					p.release(len(run))
					return
				}
			}
			err = &ProtocolError{Msg: fmt.Sprintf("unexpected reply to %s: %s", opName(req), opName(resp))}
		}
		p.fail(run, err)
	})
	if err != nil {
		p.fail(run, err)
	}
}

func (p *Publisher) fail(run []*pubItem, err error) {
	for _, it := range run {
		it.ack.resolve(PublishResult{}, err)
	}
	p.release(len(run))
}

func (p *Publisher) release(n int) {
	for i := 0; i < n; i++ {
		<-p.sem
	}
	p.mu.Lock()
	p.inFlight -= n
	if p.inFlight == 0 && p.idle != nil {
		close(p.idle)
		p.idle = nil
	}
	p.mu.Unlock()
}
