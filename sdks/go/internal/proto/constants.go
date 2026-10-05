// Package proto implements the binary encoding of Exspeed client protocol
// v2 (docs/protocol.md), in both directions: requests and responses are
// encoded and decoded, so the unit tests can run a fake server on the same
// codec. It mirrors crates/exspeed-protocol/src/client.rs.
package proto

// Version is the protocol version byte that starts every frame.
const Version byte = 0x02

// HeaderSize is the size of a frame header:
// [version u8][opcode u8][correlation id u32 LE][payload length u32 LE].
const HeaderSize = 10

// MaxPayload is the largest frame payload either side accepts (16 MiB).
const MaxPayload = 16 * 1024 * 1024

// DefaultPort is the default client port.
const DefaultPort = 5933

// Opcodes of client protocol v2. Requests use 0x01-0x7F (plus Ping);
// responses and pushes use 0x80-0xFF.
const (
	OpConnect        byte = 0x01
	OpMetadata       byte = 0x03
	OpPublish        byte = 0x10
	OpPublishBatch   byte = 0x11
	OpCreateStream   byte = 0x18
	OpUpdateStream   byte = 0x19
	OpDeleteStream   byte = 0x1A
	OpStreamInfo     byte = 0x1B
	OpListStreams    byte = 0x1C
	OpQuery          byte = 0x20
	OpCreateConsumer byte = 0x40
	OpDeleteConsumer byte = 0x41
	OpConsumerInfo   byte = 0x42
	OpListConsumers  byte = 0x43
	OpSeekConsumer   byte = 0x44
	OpSubscribe      byte = 0x50
	OpCredit         byte = 0x51
	OpUnsubscribe    byte = 0x52
	OpPull           byte = 0x53
	OpAck            byte = 0x54
	OpNack           byte = 0x55
	OpTerm           byte = 0x56
	OpInProgress     byte = 0x57
	OpRead           byte = 0x60
	OpCorePublish    byte = 0x70
	OpCoreSubscribe  byte = 0x71
	OpKvPut          byte = 0x74
	OpKvGet          byte = 0x75
	OpKvDelete       byte = 0x76
	OpKvKeys         byte = 0x77
	OpKvHistory      byte = 0x78
	OpKvCreateBucket byte = 0x79
	OpPing           byte = 0xF0

	OpOk                byte = 0x80
	OpError             byte = 0x81
	OpDeliver           byte = 0x82
	OpMessages          byte = 0x83
	OpReadResult        byte = 0x84
	OpJSON              byte = 0x85
	OpPublishOk         byte = 0x86
	OpPublishBatchOk    byte = 0x87
	OpConnectOk         byte = 0x88
	OpSubscribeOk       byte = 0x89
	OpSubscriptionEnded byte = 0x8A
	OpCoreMsg           byte = 0x8B
	OpPong              byte = 0xF1
)

// Seek kinds of a SeekConsumer request.
const (
	SeekEarliest byte = 0
	SeekLatest   byte = 1
	SeekOffset   byte = 2
	SeekTime     byte = 3
)

// Record layout constants (crates/exspeed-common/src/record_format.rs).
const (
	recCRCAt      = 4
	recDeliveryAt = 8
	recOffsetAt   = 10
	recTimeAt     = 18
	recSubjectAt  = 26
	// MinRecordLen is the smallest possible encoded record.
	MinRecordLen = 10 + 8 + 8 + 2 + 1 + 4 + 2
	// MaxRecordLen is the largest encoded record a decoder accepts.
	MaxRecordLen = 64 * 1024 * 1024
)
