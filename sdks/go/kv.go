package exspeed

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alternayte/exspeed/sdks/go/internal/proto"
)

// KVOpHeader marks a KV tombstone: "DEL" or "PURGE".
const KVOpHeader = "exspeed-kv-op"

// KVOp is what a revision holds.
type KVOp string

// KV operations.
const (
	KVPut    KVOp = "put"
	KVDelete KVOp = "delete"
	KVPurge  KVOp = "purge"
)

// KVEntry is one revision of a key.
type KVEntry struct {
	Key   string
	Value []byte
	// Revision is the record's offset in the bucket's stream plus one
	// (0 means absent).
	Revision uint64
	// Time is the write time.
	Time time.Time
	// Op is KVPut, or KVDelete / KVPurge for a tombstone (empty value).
	Op KVOp
}

func kvEntryFromWire(r *proto.WireRecord) *KVEntry {
	e := &KVEntry{
		Key:      r.Subject,
		Value:    r.Value,
		Revision: r.Offset + 1,
		Time:     time.Unix(0, int64(r.TimestampNs)),
		Op:       KVPut,
	}
	for _, h := range r.Headers {
		if h.Key == KVOpHeader {
			switch h.Value {
			case "DEL":
				e.Op = KVDelete
			case "PURGE":
				e.Op = KVPurge
			}
			break
		}
	}
	return e
}

// Text is the value as a string.
func (e *KVEntry) Text() string { return string(e.Value) }

// JSON unmarshals the value into v.
func (e *KVEntry) JSON(v any) error { return json.Unmarshal(e.Value, v) }

// KVBucketOptions configure [KV.Create]. Zero values are the defaults.
type KVBucketOptions struct {
	// History is how many values each key keeps, 1 to 64. Default 1.
	History int
	// TTL expires keys this long after their last put (whole ms). Default: never.
	TTL time.Duration
	// MaxBytes is the bucket's size limit. Default: the server's.
	MaxBytes uint64
}

// KVPutOptions configure [KV.PutWith].
type KVPutOptions struct {
	// TTL expires this key this long after the put (whole ms).
	TTL time.Duration
	// ExpectedRevision writes only if the key is at this revision (0 =
	// absent), else the put fails with 409 ([ErrConflict]).
	ExpectedRevision *uint64
}

// KVDeleteOptions configure [KV.DeleteWith].
type KVDeleteOptions struct {
	// Purge also hides the key's older values from its history.
	Purge bool
	// ExpectedRevision deletes only if the key is at this revision, else
	// 409 ([ErrConflict]).
	ExpectedRevision *uint64
}

// KV is a key-value bucket, from [Client.KV]. The bucket B is the stream
// KV_B: each key is a subject, each put a record, and a key's revision is
// its record's offset plus one. Keys are dot-separated tokens (app.mode).
type KV struct {
	c      *Client
	bucket string
}

// Bucket is the bucket's name.
func (kv *KV) Bucket() string { return kv.bucket }

// Stream is the bucket's stream, KV_<bucket>.
func (kv *KV) Stream() string { return "KV_" + kv.bucket }

// Create creates the bucket. It is idempotent for the same settings.
func (kv *KV) Create(ctx context.Context, opts KVBucketOptions) error {
	if opts.History < 0 || opts.TTL < 0 {
		return invalidf("History and TTL must not be negative")
	}
	return kv.c.callOk(ctx, proto.KvCreateBucket{
		Bucket:   kv.bucket,
		History:  uint64(opts.History),
		TTLMs:    ceilUnits(opts.TTL, time.Millisecond),
		MaxBytes: opts.MaxBytes,
	})
}

// Destroy deletes the bucket and every key in it.
func (kv *KV) Destroy(ctx context.Context) error { return kv.c.DeleteStream(ctx, kv.Stream()) }

// Get returns the current value of key, or nil (and no error) when the key
// is absent, deleted or expired. A missing bucket is an error (404).
func (kv *KV) Get(ctx context.Context, key string) (*KVEntry, error) {
	return kv.get(ctx, key, nil)
}

// GetRevision returns key at revision while the bucket still keeps it, or
// nil.
func (kv *KV) GetRevision(ctx context.Context, key string, revision uint64) (*KVEntry, error) {
	return kv.get(ctx, key, &revision)
}

func (kv *KV) get(ctx context.Context, key string, revision *uint64) (*KVEntry, error) {
	r, err := call[proto.Messages](ctx, kv.c, proto.KvGet{Bucket: kv.bucket, Key: key, Revision: revision}, reqOpts{})
	if err != nil {
		// 404 is "key '...' not found" or "bucket '...' not found".
		var se *ServerError
		if errors.As(err, &se) && se.Code == CodeNotFound && strings.HasPrefix(se.Message, "key ") {
			return nil, nil
		}
		return nil, err
	}
	if len(r.Records) == 0 {
		return nil, nil
	}
	return kvEntryFromWire(&r.Records[0]), nil
}

// Put sets key and returns the new revision.
func (kv *KV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return kv.PutWith(ctx, key, value, KVPutOptions{})
}

// PutWith sets key with options (a TTL, a compare-and-set revision) and
// returns the new revision.
func (kv *KV) PutWith(ctx context.Context, key string, value []byte, opts KVPutOptions) (uint64, error) {
	if opts.TTL < 0 {
		return 0, invalidf("TTL must not be negative")
	}
	if value == nil {
		value = []byte{}
	}
	req := proto.KvPut{Bucket: kv.bucket, Key: key, Value: value, ExpectedRevision: opts.ExpectedRevision}
	if opts.TTL > 0 {
		req.TTLMs = ptr(ceilUnits(opts.TTL, time.Millisecond))
	}
	return kv.write(ctx, req)
}

// CreateKey sets key only if it doesn't exist (or was deleted); otherwise
// it fails with 409 ([ErrConflict]).
func (kv *KV) CreateKey(ctx context.Context, key string, value []byte) (uint64, error) {
	return kv.PutWith(ctx, key, value, KVPutOptions{ExpectedRevision: ptr(uint64(0))})
}

// Update sets key only if it is at revision (compare-and-set); otherwise it
// fails with 409 ([ErrConflict]) and [ServerError.CurrentRevision].
func (kv *KV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return kv.PutWith(ctx, key, value, KVPutOptions{ExpectedRevision: &revision})
}

// Delete deletes key (its history stays until it ages out) and returns the
// tombstone's revision.
func (kv *KV) Delete(ctx context.Context, key string) (uint64, error) {
	return kv.DeleteWith(ctx, key, KVDeleteOptions{})
}

// Purge deletes key and hides its older values, returning the tombstone's
// revision.
func (kv *KV) Purge(ctx context.Context, key string) (uint64, error) {
	return kv.DeleteWith(ctx, key, KVDeleteOptions{Purge: true})
}

// DeleteWith deletes or purges key with options.
func (kv *KV) DeleteWith(ctx context.Context, key string, opts KVDeleteOptions) (uint64, error) {
	return kv.write(ctx, proto.KvDelete{Bucket: kv.bucket, Key: key, Purge: opts.Purge, ExpectedRevision: opts.ExpectedRevision})
}

func (kv *KV) write(ctx context.Context, m proto.Message) (uint64, error) {
	r, err := call[proto.PublishOk](ctx, kv.c, m, reqOpts{})
	if err != nil {
		return 0, err
	}
	return r.Offset, nil
}

// Keys lists the keys that have a value, matching filter (NATS-style;
// "" = all), sorted.
func (kv *KV) Keys(ctx context.Context, filter string) ([]string, error) {
	var keys []string
	if err := kv.c.callJSON(ctx, proto.KvKeys{Bucket: kv.bucket, Filter: filter}, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// History lists the kept revisions of key, oldest first, deletes included.
func (kv *KV) History(ctx context.Context, key string) ([]*KVEntry, error) {
	r, err := call[proto.Messages](ctx, kv.c, proto.KvHistory{Bucket: kv.bucket, Key: key}, reqOpts{})
	if err != nil {
		return nil, err
	}
	out := make([]*KVEntry, len(r.Records))
	for i := range r.Records {
		out[i] = kvEntryFromWire(&r.Records[i])
	}
	return out, nil
}

// Watch watches keys matching filter ("" = all): first the current value of
// every matching key (deleted keys left out), ordered by revision, then
// every change as it happens, deletes included. Call [KVWatch.Stop] when
// done.
func (kv *KV) Watch(filter string) *KVWatch {
	ctx, cancel := context.WithCancel(kv.c.ctx)
	return &KVWatch{kv: kv, filter: filter, ctx: ctx, cancel: cancel}
}

const (
	watchWait  = 10 * time.Second
	watchBatch = 1000
)

// KVWatch is a stream of changes to a bucket's keys, from [KV.Watch]. It
// reads the bucket's stream with stateless long-poll reads, so it holds no
// state on the server, and it resumes where it was after a reconnect.
type KVWatch struct {
	kv     *KV
	filter string
	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	from         uint64
	snapshotDone bool
	pending      []*KVEntry
	fetching     chan struct{} // non-nil while a read runs; closed when it ends
	fetchErr     error
	stopped      bool
}

// Next returns the next entry, waiting for changes as long as ctx allows
// (ctx.Err() when it is done first; a read in progress keeps running and
// its entries are kept for the next call). A failed read, such as a
// [*ConnectionError] while the connection is down, is returned once; the
// next call reads again. After Stop it returns [ErrWatchStopped].
func (w *KVWatch) Next(ctx context.Context) (*KVEntry, error) {
	for {
		w.mu.Lock()
		if w.stopped {
			w.mu.Unlock()
			return nil, ErrWatchStopped
		}
		if len(w.pending) > 0 {
			e := w.pending[0]
			w.pending[0] = nil
			w.pending = w.pending[1:]
			w.mu.Unlock()
			return e, nil
		}
		if w.fetchErr != nil {
			err := w.fetchErr
			w.fetchErr = nil
			w.mu.Unlock()
			return nil, err
		}
		if w.fetching == nil {
			w.fetching = make(chan struct{})
			go w.fetch(w.fetching, w.snapshotDone)
		}
		done := w.fetching
		w.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Stop ends the watch and its background read.
func (w *KVWatch) Stop() {
	w.mu.Lock()
	w.stopped = true
	w.pending = nil
	w.mu.Unlock()
	w.cancel()
}

// Closed reports whether Stop was called.
func (w *KVWatch) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

func (w *KVWatch) fetch(done chan struct{}, snapshotDone bool) {
	var entries []*KVEntry
	var err error
	if snapshotDone {
		entries, err = w.follow()
	} else {
		entries, err = w.loadSnapshot()
	}
	w.mu.Lock()
	if !w.stopped {
		if err != nil {
			w.fetchErr = err
		} else {
			w.pending = append(w.pending, entries...)
			if !snapshotDone {
				w.snapshotDone = true
			}
		}
	}
	w.fetching = nil
	close(done)
	w.mu.Unlock()
}

func (w *KVWatch) position() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.from
}

func (w *KVWatch) advance(next uint64) {
	w.mu.Lock()
	if next > w.from {
		w.from = next
	}
	w.mu.Unlock()
}

// loadSnapshot reads up to the high watermark seen by the first read, keeps
// the last record per key and returns the live keys sorted by revision.
func (w *KVWatch) loadSnapshot() ([]*KVEntry, error) {
	latest := map[string]*KVEntry{}
	var end uint64
	first := true
	for {
		from := w.position()
		r, err := w.kv.c.Read(w.ctx, w.kv.Stream(), ReadOptions{From: from, MaxRecords: watchBatch, Filter: w.filter})
		if err != nil {
			return nil, err
		}
		if first {
			end, first = r.HighWatermark, false
		}
		for i := range r.Records {
			rec := &r.Records[i]
			latest[rec.Subject] = &KVEntry{
				Key: rec.Subject, Value: rec.Value, Revision: rec.Offset + 1, Time: rec.Time, Op: opOf(rec.Headers),
			}
		}
		w.advance(r.NextOffset)
		if w.position() >= end || r.NextOffset <= from {
			break
		}
	}
	live := make([]*KVEntry, 0, len(latest))
	for _, e := range latest {
		if e.Op == KVPut {
			live = append(live, e)
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Revision < live[j].Revision })
	return live, nil
}

func (w *KVWatch) follow() ([]*KVEntry, error) {
	r, err := w.kv.c.Read(w.ctx, w.kv.Stream(), ReadOptions{From: w.position(), MaxRecords: watchBatch, Wait: watchWait, Filter: w.filter})
	if err != nil {
		return nil, err
	}
	w.advance(r.NextOffset)
	out := make([]*KVEntry, len(r.Records))
	for i := range r.Records {
		rec := &r.Records[i]
		out[i] = &KVEntry{Key: rec.Subject, Value: rec.Value, Revision: rec.Offset + 1, Time: rec.Time, Op: opOf(rec.Headers)}
	}
	return out, nil
}

func opOf(h []Header) KVOp {
	v, _ := findHeader(h, KVOpHeader)
	switch v {
	case "DEL":
		return KVDelete
	case "PURGE":
		return KVPurge
	}
	return KVPut
}
