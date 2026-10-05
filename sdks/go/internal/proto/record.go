package proto

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"unicode/utf8"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC32C is the CRC32C (Castagnoli) checksum of b.
func CRC32C(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// WireRecord is a record as the server delivers it (and stores it on
// disk): u32 len, u32 crc, u16 delivery_count, u64 offset, u64
// timestamp_ns, then subject, key, value and headers.
type WireRecord struct {
	Offset uint64
	// TimestampNs is the append time in nanoseconds since the Unix epoch.
	TimestampNs uint64
	// DeliveryCount is 1 on first delivery, 0 for stateless reads.
	DeliveryCount uint16
	Subject       string
	// Key is nil when the record has no key.
	Key     []byte
	Value   []byte
	Headers []Header
}

// EncodedLen is the size of r's wire encoding, or an error when a field is
// too long for its length prefix.
func (r *WireRecord) EncodedLen() (int, error) {
	if len(r.Subject) > math.MaxUint16 {
		return 0, &EncodeError{Msg: "record subject is too long"}
	}
	if len(r.Headers) > math.MaxUint16 {
		return 0, &EncodeError{Msg: "too many record headers"}
	}
	n := MinRecordLen + len(r.Subject) + len(r.Value)
	if r.Key != nil {
		n += 4 + len(r.Key)
	}
	for _, h := range r.Headers {
		if len(h.Key) > math.MaxUint16 || len(h.Value) > math.MaxUint16 {
			return 0, &EncodeError{Msg: "record header is too long"}
		}
		n += 4 + len(h.Key) + len(h.Value)
	}
	if n > MaxRecordLen {
		return 0, &EncodeError{Msg: "encoded record is too large"}
	}
	return n, nil
}

// Record appends the wire encoding of r, CRC included.
func (w *Writer) Record(r *WireRecord) {
	size, err := r.EncodedLen()
	if err != nil {
		w.fail("record at offset %d: %v", r.Offset, err)
		return
	}
	start := len(w.buf)
	w.U32(uint32(size - 4))
	w.U32(0) // CRC, filled in below
	w.U16(r.DeliveryCount)
	w.U64(r.Offset)
	w.U64(r.TimestampNs)
	w.Str(r.Subject)
	w.OptBytes(r.Key)
	w.Bytes(r.Value)
	w.Headers(r.Headers)
	rec := w.buf[start:]
	binary.LittleEndian.PutUint32(rec[recCRCAt:], CRC32C(rec[recOffsetAt:]))
}

// EncodeRecord returns the wire encoding of one record.
func EncodeRecord(r *WireRecord) ([]byte, error) {
	w := NewWriter(64 + len(r.Value))
	w.Record(r)
	return w.Finish()
}

// VerifyRecordCRC reports whether one complete encoded record carries a
// valid CRC32C (over the bytes after delivery_count).
func VerifyRecordCRC(rec []byte) bool {
	if len(rec) < MinRecordLen {
		return false
	}
	return binary.LittleEndian.Uint32(rec[recCRCAt:]) == CRC32C(rec[recOffsetAt:])
}

// Record decodes one record, verifying its length field, structure and CRC.
// Byte slices in the result alias the payload.
func (r *Reader) Record() (WireRecord, error) {
	if err := r.need(4); err != nil {
		return WireRecord{}, err
	}
	size := int(binary.LittleEndian.Uint32(r.buf[r.pos:])) + 4
	if size < MinRecordLen || size > MaxRecordLen {
		return WireRecord{}, decodeErr("record: record length %d out of bounds", size)
	}
	rec, err := r.take(size)
	if err != nil {
		return WireRecord{}, err
	}
	if !VerifyRecordCRC(rec) {
		return WireRecord{}, decodeErr("record: CRC mismatch: stored %#010x, computed %#010x",
			binary.LittleEndian.Uint32(rec[recCRCAt:]), CRC32C(rec[recOffsetAt:]))
	}
	out := WireRecord{
		DeliveryCount: binary.LittleEndian.Uint16(rec[recDeliveryAt:]),
		Offset:        binary.LittleEndian.Uint64(rec[recOffsetAt:]),
		TimestampNs:   binary.LittleEndian.Uint64(rec[recTimeAt:]),
	}
	c := &Reader{buf: rec, pos: recSubjectAt}
	bad := func(err error) (WireRecord, error) {
		return WireRecord{}, decodeErr("record: %v", err)
	}
	n, err := c.U16()
	if err != nil {
		return bad(err)
	}
	subj, err := c.take(int(n))
	if err != nil {
		return bad(err)
	}
	if !utf8.Valid(subj) {
		return bad(decodeErr("invalid subject UTF-8"))
	}
	out.Subject = string(subj)
	flag, err := c.U8()
	if err != nil {
		return bad(err)
	}
	switch flag {
	case 0:
	case 1:
		if out.Key, err = c.Bytes(); err != nil {
			return bad(err)
		}
	default:
		return bad(decodeErr("invalid key flag %d", flag))
	}
	if out.Value, err = c.Bytes(); err != nil {
		return bad(err)
	}
	if out.Headers, err = c.Headers(); err != nil {
		return bad(err)
	}
	if c.Remaining() != 0 {
		return bad(decodeErr("%d trailing bytes after record", c.Remaining()))
	}
	return out, nil
}

// Records decodes a vec<WireRecord>.
func (r *Reader) Records() ([]WireRecord, error) {
	n, err := r.Count(MinRecordLen)
	if err != nil {
		return nil, err
	}
	out := make([]WireRecord, 0, n)
	for i := 0; i < n; i++ {
		rec, err := r.Record()
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// Records writes a vec<WireRecord>.
func (w *Writer) Records(recs []WireRecord) {
	w.U32(uint32(len(recs)))
	for i := range recs {
		w.Record(&recs[i])
	}
}
