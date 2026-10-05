package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Frame is one protocol frame: header fields plus the raw payload.
type Frame struct {
	Opcode        byte
	CorrelationID uint32
	Payload       []byte
}

// FrameTooLargeError reports a payload above MaxPayload.
type FrameTooLargeError struct{ Size int }

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("payload too large: %d bytes (max %d)", e.Size, MaxPayload)
}

// EncodeFrame serializes a frame: [version][opcode][corr u32 LE][len u32 LE][payload].
func EncodeFrame(opcode byte, corr uint32, payload []byte) ([]byte, error) {
	if len(payload) > MaxPayload {
		return nil, &FrameTooLargeError{Size: len(payload)}
	}
	buf := make([]byte, HeaderSize, HeaderSize+len(payload))
	putHeader(buf, opcode, corr, len(payload))
	return append(buf, payload...), nil
}

func putHeader(buf []byte, opcode byte, corr uint32, n int) {
	buf[0] = Version
	buf[1] = opcode
	binary.LittleEndian.PutUint32(buf[2:], corr)
	binary.LittleEndian.PutUint32(buf[6:], uint32(n))
}

// frameWriter starts a writer with room for the frame header.
func frameWriter(capacity int) *Writer {
	w := NewWriter(HeaderSize + capacity)
	w.buf = w.buf[:HeaderSize]
	return w
}

// finishFrame fills in the header of a frameWriter's buffer.
func finishFrame(w *Writer, opcode byte, corr uint32) ([]byte, error) {
	buf, err := w.Finish()
	if err != nil {
		return nil, err
	}
	n := len(buf) - HeaderSize
	if n > MaxPayload {
		return nil, &FrameTooLargeError{Size: n}
	}
	putHeader(buf, opcode, corr, n)
	return buf, nil
}

// ReadFrame reads one frame from r. A bad version or an oversize length is
// a *DecodeError: the byte stream can't be resynchronised after that, so the
// caller should drop the connection.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	if hdr[0] != Version {
		return Frame{}, decodeErr("unsupported protocol version 0x%x", hdr[0])
	}
	n := binary.LittleEndian.Uint32(hdr[6:])
	if n > MaxPayload {
		return Frame{}, decodeErr("payload too large: %d bytes (max %d)", n, MaxPayload)
	}
	f := Frame{Opcode: hdr[1], CorrelationID: binary.LittleEndian.Uint32(hdr[2:])}
	if n > 0 {
		f.Payload = make([]byte, n)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return Frame{}, err
		}
	}
	return f, nil
}
