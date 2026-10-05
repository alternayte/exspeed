using System.Buffers.Binary;
using System.Text;

namespace Exspeed.Protocol;

/// <summary>
/// Little-endian writer for the encodings of client protocol v2, mirroring <c>Writer</c> in
/// <c>crates/exspeed-protocol/src/client.rs</c>:
/// <c>str</c> = u16 length + UTF-8, <c>lstr</c> = u32 length + UTF-8, <c>bytes</c> = u32 length + raw,
/// <c>opt&lt;T&gt;</c> = u8 flag + T, <c>headers</c> = u16 count + (str, str) pairs, <c>vec&lt;T&gt;</c> = u32 count + items.
/// A field that does not fit its length prefix is never truncated: it throws.
/// </summary>
internal sealed class WireWriter
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private byte[] _buf;
    private int _pos;

    public WireWriter(int initialSize = 128)
    {
        _buf = new byte[Math.Max(16, initialSize)];
    }

    /// <summary>A writer that reserves room for a frame header (see <see cref="FinishFrame"/>).</summary>
    public static WireWriter ForFrame(int initialSize = 128)
    {
        var w = new WireWriter(initialSize + ProtocolConstants.FrameHeaderSize);
        w._pos = ProtocolConstants.FrameHeaderSize;
        return w;
    }

    public int Length => _pos;

    public ReadOnlySpan<byte> WrittenSpan => _buf.AsSpan(0, _pos);

    public byte[] ToArray() => _buf.AsSpan(0, _pos).ToArray();

    /// <summary>Fill in the header reserved by <see cref="ForFrame"/> and return the frame bytes.</summary>
    public byte[] FinishFrame(OpCode opcode, uint correlationId)
    {
        int payload = _pos - ProtocolConstants.FrameHeaderSize;
        if (payload > ProtocolConstants.MaxPayloadSize)
        {
            throw new ExspeedException(
                $"payload too large: {payload} bytes (max {ProtocolConstants.MaxPayloadSize}); split the batch");
        }
        _buf[0] = ProtocolConstants.Version;
        _buf[1] = (byte)opcode;
        BinaryPrimitives.WriteUInt32LittleEndian(_buf.AsSpan(2), correlationId);
        BinaryPrimitives.WriteUInt32LittleEndian(_buf.AsSpan(6), (uint)payload);
        return _buf.AsSpan(0, _pos).ToArray();
    }

    private Span<byte> Grow(int n)
    {
        int need = _pos + n;
        if (need > _buf.Length)
        {
            int size = _buf.Length * 2;
            while (size < need)
            {
                size *= 2;
            }
            Array.Resize(ref _buf, size);
        }
        var span = _buf.AsSpan(_pos, n);
        _pos += n;
        return span;
    }

    public WireWriter U8(byte v)
    {
        Grow(1)[0] = v;
        return this;
    }

    public WireWriter Bool(bool v) => U8(v ? (byte)1 : (byte)0);

    public WireWriter U16(ushort v)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(Grow(2), v);
        return this;
    }

    public WireWriter U32(uint v)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(Grow(4), v);
        return this;
    }

    public WireWriter U64(ulong v)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(Grow(8), v);
        return this;
    }

    public WireWriter Raw(ReadOnlySpan<byte> b)
    {
        b.CopyTo(Grow(b.Length));
        return this;
    }

    public WireWriter Str(string s)
    {
        int n = Utf8.GetByteCount(s);
        if (n > ushort.MaxValue)
        {
            string head = s.Length > 40 ? s[..40] : s;
            throw new ExspeedException($"string too long ({n} bytes, max 65535): {head}...");
        }
        U16((ushort)n);
        Utf8.GetBytes(s, Grow(n));
        return this;
    }

    public WireWriter LStr(string s)
    {
        int n = Utf8.GetByteCount(s);
        U32((uint)n);
        Utf8.GetBytes(s, Grow(n));
        return this;
    }

    public WireWriter Bytes(ReadOnlySpan<byte> b)
    {
        U32((uint)b.Length);
        return Raw(b);
    }

    public WireWriter OptStr(string? s)
    {
        if (s is null)
        {
            return U8(0);
        }
        U8(1);
        return Str(s);
    }

    public WireWriter OptBytes(byte[]? b)
    {
        if (b is null)
        {
            return U8(0);
        }
        U8(1);
        return Bytes(b);
    }

    public WireWriter OptU64(ulong? v)
    {
        if (v is null)
        {
            return U8(0);
        }
        U8(1);
        return U64(v.Value);
    }

    public WireWriter Headers(IReadOnlyList<KeyValuePair<string, string>> headers)
    {
        if (headers.Count > ushort.MaxValue)
        {
            throw new ExspeedException($"too many headers ({headers.Count}, max 65535)");
        }
        U16((ushort)headers.Count);
        foreach (var (k, v) in headers)
        {
            Str(k);
            Str(v);
        }
        return this;
    }
}
