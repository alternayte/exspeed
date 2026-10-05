using System.Buffers.Binary;

namespace Exspeed.Protocol;

/// <summary>One protocol frame: header fields plus the raw payload.</summary>
internal readonly record struct Frame(byte Opcode, uint CorrelationId, ReadOnlyMemory<byte> Payload);

internal static class Frames
{
    /// <summary>Serialize a frame: <c>[version][opcode][corr u32 LE][len u32 LE][payload]</c>.</summary>
    public static byte[] Encode(byte opcode, uint correlationId, ReadOnlySpan<byte> payload)
    {
        if (payload.Length > ProtocolConstants.MaxPayloadSize)
        {
            throw new ExspeedException(
                $"payload too large: {payload.Length} bytes (max {ProtocolConstants.MaxPayloadSize}); split the batch");
        }
        var buf = new byte[ProtocolConstants.FrameHeaderSize + payload.Length];
        buf[0] = ProtocolConstants.Version;
        buf[1] = opcode;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(2), correlationId);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(6), (uint)payload.Length);
        payload.CopyTo(buf.AsSpan(ProtocolConstants.FrameHeaderSize));
        return buf;
    }
}

/// <summary>
/// Incremental frame parser for a byte stream. Feed it socket chunks; it returns every complete frame.
/// A bad version or an oversize length throws an <see cref="ExspeedProtocolException"/>: the stream cannot be
/// resynchronised after that, so the caller should drop the connection.
/// </summary>
internal sealed class FrameParser
{
    private byte[] _buf = new byte[64 * 1024];
    private int _start;
    private int _end;

    /// <summary>Bytes buffered towards an incomplete frame.</summary>
    public int Pending => _end - _start;

    /// <summary>A span to receive into (at least <paramref name="min"/> bytes); call <see cref="Advance"/> after.</summary>
    public Memory<byte> GetWriteBuffer(int min = 4096)
    {
        if (_buf.Length - _end < min)
        {
            int pending = _end - _start;
            if (pending + min <= _buf.Length && _start > 0)
            {
                Buffer.BlockCopy(_buf, _start, _buf, 0, pending);
            }
            else
            {
                int size = _buf.Length;
                while (size < pending + min)
                {
                    size *= 2;
                }
                var next = new byte[size];
                Buffer.BlockCopy(_buf, _start, next, 0, pending);
                _buf = next;
            }
            _start = 0;
            _end = pending;
        }
        return _buf.AsMemory(_end);
    }

    /// <summary>Mark <paramref name="n"/> bytes of the write buffer as received.</summary>
    public void Advance(int n) => _end += n;

    /// <summary>Append a chunk and return every complete frame.</summary>
    public List<Frame> Push(ReadOnlySpan<byte> chunk)
    {
        var dst = GetWriteBuffer(chunk.Length);
        chunk.CopyTo(dst.Span);
        Advance(chunk.Length);
        var frames = new List<Frame>();
        Drain(frames);
        return frames;
    }

    /// <summary>Move every complete buffered frame into <paramref name="frames"/>.</summary>
    public void Drain(List<Frame> frames)
    {
        while (_end - _start >= ProtocolConstants.FrameHeaderSize)
        {
            var header = _buf.AsSpan(_start, ProtocolConstants.FrameHeaderSize);
            byte version = header[0];
            if (version != ProtocolConstants.Version)
            {
                throw new ExspeedProtocolException($"unsupported protocol version 0x{version:x}");
            }
            byte opcode = header[1];
            uint corr = BinaryPrimitives.ReadUInt32LittleEndian(header[2..]);
            uint len = BinaryPrimitives.ReadUInt32LittleEndian(header[6..]);
            if (len > ProtocolConstants.MaxPayloadSize)
            {
                throw new ExspeedProtocolException(
                    $"payload too large: {len} bytes (max {ProtocolConstants.MaxPayloadSize})");
            }
            int total = ProtocolConstants.FrameHeaderSize + (int)len;
            if (_end - _start < total)
            {
                break;
            }
            var payload = _buf.AsSpan(_start + ProtocolConstants.FrameHeaderSize, (int)len).ToArray();
            frames.Add(new Frame(opcode, corr, payload));
            _start += total;
        }
        if (_start == _end)
        {
            _start = 0;
            _end = 0;
        }
    }
}
