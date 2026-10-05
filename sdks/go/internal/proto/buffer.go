package proto

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"
)

// DecodeError reports bytes that are not a valid encoding: truncated or
// trailing payloads, bad lengths, invalid UTF-8 or a CRC mismatch.
type DecodeError struct{ Msg string }

func (e *DecodeError) Error() string { return e.Msg }

func decodeErr(format string, args ...any) error {
	return &DecodeError{Msg: fmt.Sprintf(format, args...)}
}

// EncodeError reports a value that doesn't fit its wire type (for example a
// string longer than 65,535 bytes). Nothing is truncated.
type EncodeError struct{ Msg string }

func (e *EncodeError) Error() string { return e.Msg }

// Header is one record or message header. Keys may repeat.
type Header struct {
	Key   string
	Value string
}

// Writer is a little-endian encoder. A value that doesn't fit its length
// prefix is never truncated: the writer remembers the first such error
// and Finish returns it.
type Writer struct {
	buf []byte
	err error
}

// NewWriter returns a writer with the given initial capacity.
func NewWriter(capacity int) *Writer { return &Writer{buf: make([]byte, 0, capacity)} }

func (w *Writer) fail(format string, args ...any) {
	if w.err == nil {
		w.err = &EncodeError{Msg: fmt.Sprintf(format, args...)}
	}
}

// Finish returns the encoded bytes, or the first encoding error.
func (w *Writer) Finish() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.buf, nil
}

// Len is the number of bytes written so far.
func (w *Writer) Len() int { return len(w.buf) }

// U8 writes one byte.
func (w *Writer) U8(v byte) { w.buf = append(w.buf, v) }

// U16 writes a little-endian u16.
func (w *Writer) U16(v uint16) { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }

// U32 writes a little-endian u32.
func (w *Writer) U32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

// U64 writes a little-endian u64.
func (w *Writer) U64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }

// Raw writes bytes without a length prefix.
func (w *Writer) Raw(b []byte) { w.buf = append(w.buf, b...) }

// Bool writes a u8 flag.
func (w *Writer) Bool(v bool) {
	if v {
		w.U8(1)
	} else {
		w.U8(0)
	}
}

// Str writes a u16 length and the UTF-8 bytes.
func (w *Writer) Str(s string) {
	if len(s) > math.MaxUint16 {
		w.fail("string of %d bytes exceeds the %d byte limit", len(s), math.MaxUint16)
		return
	}
	w.U16(uint16(len(s)))
	w.buf = append(w.buf, s...)
}

// LStr writes a u32 length and the UTF-8 bytes (SQL text).
func (w *Writer) LStr(s string) {
	if uint64(len(s)) > math.MaxUint32 {
		w.fail("string of %d bytes is too long", len(s))
		return
	}
	w.U32(uint32(len(s)))
	w.buf = append(w.buf, s...)
}

// Bytes writes a u32 length and the raw bytes.
func (w *Writer) Bytes(b []byte) {
	if uint64(len(b)) > math.MaxUint32 {
		w.fail("byte string of %d bytes is too long", len(b))
		return
	}
	w.U32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}

// OptStr writes an optional string: a u8 flag, then the string when present.
func (w *Writer) OptStr(s *string) {
	if s == nil {
		w.U8(0)
		return
	}
	w.U8(1)
	w.Str(*s)
}

// OptBytes writes optional bytes; nil is absent.
func (w *Writer) OptBytes(b []byte) {
	if b == nil {
		w.U8(0)
		return
	}
	w.U8(1)
	w.Bytes(b)
}

// OptU64 writes an optional u64.
func (w *Writer) OptU64(v *uint64) {
	if v == nil {
		w.U8(0)
		return
	}
	w.U8(1)
	w.U64(*v)
}

// Headers writes a u16 count and (key, value) string pairs.
func (w *Writer) Headers(h []Header) {
	if len(h) > math.MaxUint16 {
		w.fail("%d headers exceed the limit of %d", len(h), math.MaxUint16)
		return
	}
	w.U16(uint16(len(h)))
	for _, kv := range h {
		w.Str(kv.Key)
		w.Str(kv.Value)
	}
}

// Reader is a bounds-checked little-endian decoder over one payload.
type Reader struct {
	buf []byte
	pos int
}

// NewReader reads from b.
func NewReader(b []byte) *Reader { return &Reader{buf: b} }

// Remaining is the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.buf) - r.pos }

func (r *Reader) need(n int) error {
	if r.Remaining() < n {
		return decodeErr("truncated payload: need %d bytes, have %d", n, r.Remaining())
	}
	return nil
}

// Finish fails if bytes are left over.
func (r *Reader) Finish() error {
	if r.Remaining() > 0 {
		return decodeErr("%d trailing bytes", r.Remaining())
	}
	return nil
}

// U8 reads one byte.
func (r *Reader) U8() (byte, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

// U16 reads a little-endian u16.
func (r *Reader) U16() (uint16, error) {
	if err := r.need(2); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// U32 reads a little-endian u32.
func (r *Reader) U32() (uint32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// U64 reads a little-endian u64.
func (r *Reader) U64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

// Bool reads a u8 flag (any non-zero value is true).
func (r *Reader) Bool() (bool, error) {
	v, err := r.U8()
	return v != 0, err
}

func (r *Reader) take(n int) ([]byte, error) {
	if err := r.need(n); err != nil {
		return nil, err
	}
	b := r.buf[r.pos : r.pos+n : r.pos+n]
	r.pos += n
	return b, nil
}

func utf8Str(b []byte) (string, error) {
	if !utf8.Valid(b) {
		return "", decodeErr("invalid UTF-8")
	}
	return string(b), nil
}

// Str reads a u16-prefixed UTF-8 string.
func (r *Reader) Str() (string, error) {
	n, err := r.U16()
	if err != nil {
		return "", err
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	return utf8Str(b)
}

// LStr reads a u32-prefixed UTF-8 string.
func (r *Reader) LStr() (string, error) {
	b, err := r.Bytes()
	if err != nil {
		return "", err
	}
	return utf8Str(b)
}

// Bytes reads u32-prefixed bytes. The result aliases the payload and is
// never nil.
func (r *Reader) Bytes() ([]byte, error) {
	n, err := r.U32()
	if err != nil {
		return nil, err
	}
	return r.take(int(n))
}

func (r *Reader) flag() (bool, error) {
	f, err := r.U8()
	if err != nil {
		return false, err
	}
	switch f {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, decodeErr("invalid option flag %d", f)
	}
}

// OptStr reads an optional string.
func (r *Reader) OptStr() (*string, error) {
	present, err := r.flag()
	if err != nil || !present {
		return nil, err
	}
	s, err := r.Str()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// OptBytes reads optional bytes (nil when absent).
func (r *Reader) OptBytes() ([]byte, error) {
	present, err := r.flag()
	if err != nil || !present {
		return nil, err
	}
	return r.Bytes()
}

// OptU64 reads an optional u64.
func (r *Reader) OptU64() (*uint64, error) {
	present, err := r.flag()
	if err != nil || !present {
		return nil, err
	}
	v, err := r.U64()
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Count reads a u32 element count, rejecting counts the remaining payload
// cannot hold (each element is at least minSize bytes).
func (r *Reader) Count(minSize int) (int, error) {
	n, err := r.U32()
	if err != nil {
		return 0, err
	}
	if minSize < 1 {
		minSize = 1
	}
	if uint64(n)*uint64(minSize) > uint64(r.Remaining()) {
		return 0, decodeErr("count %d exceeds payload size", n)
	}
	return int(n), nil
}

// Headers reads a u16 count and (key, value) string pairs.
func (r *Reader) Headers() ([]Header, error) {
	n, err := r.U16()
	if err != nil {
		return nil, err
	}
	if int(n)*4 > r.Remaining() {
		return nil, decodeErr("header count exceeds payload")
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]Header, 0, n)
	for i := 0; i < int(n); i++ {
		k, err := r.Str()
		if err != nil {
			return nil, err
		}
		v, err := r.Str()
		if err != nil {
			return nil, err
		}
		out = append(out, Header{Key: k, Value: v})
	}
	return out, nil
}
