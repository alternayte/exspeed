using System.Buffers.Binary;
using System.Text;

namespace Exspeed.Protocol;

/// <summary>
/// Bounds-checked reader for protocol v2 payloads; every failure is an
/// <see cref="ExspeedProtocolException"/>. Byte fields are returned as zero-copy slices.
/// </summary>
internal sealed class WireReader
{
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly ReadOnlyMemory<byte> _buf;
    private int _pos;

    public WireReader(ReadOnlyMemory<byte> buf)
    {
        _buf = buf;
    }

    public int Remaining => _buf.Length - _pos;

    private void Need(int n)
    {
        if (Remaining < n)
        {
            throw new ExspeedProtocolException($"truncated payload: need {n} bytes, have {Remaining}");
        }
    }

    /// <summary>Fail if bytes are left over (catches encoder/decoder drift).</summary>
    public void Finish()
    {
        if (Remaining > 0)
        {
            throw new ExspeedProtocolException($"{Remaining} trailing bytes");
        }
    }

    public byte U8()
    {
        Need(1);
        return _buf.Span[_pos++];
    }

    public bool Bool() => U8() != 0;

    public ushort U16()
    {
        Need(2);
        var v = BinaryPrimitives.ReadUInt16LittleEndian(_buf.Span[_pos..]);
        _pos += 2;
        return v;
    }

    public uint U32()
    {
        Need(4);
        var v = BinaryPrimitives.ReadUInt32LittleEndian(_buf.Span[_pos..]);
        _pos += 4;
        return v;
    }

    public ulong U64()
    {
        Need(8);
        var v = BinaryPrimitives.ReadUInt64LittleEndian(_buf.Span[_pos..]);
        _pos += 8;
        return v;
    }

    /// <summary>The next <paramref name="n"/> bytes, without a length prefix.</summary>
    public ReadOnlyMemory<byte> Raw(int n)
    {
        Need(n);
        var slice = _buf.Slice(_pos, n);
        _pos += n;
        return slice;
    }

    private static string Utf8String(ReadOnlySpan<byte> b)
    {
        try
        {
            return Utf8.GetString(b);
        }
        catch (DecoderFallbackException)
        {
            throw new ExspeedProtocolException("invalid UTF-8");
        }
    }

    public string Str() => Utf8String(Raw(U16()).Span);

    public string LStr() => Utf8String(Bytes().Span);

    public ReadOnlyMemory<byte> Bytes()
    {
        uint n = U32();
        if (n > int.MaxValue)
        {
            throw new ExspeedProtocolException($"truncated payload: need {n} bytes, have {Remaining}");
        }
        return Raw((int)n);
    }

    private bool Flag()
    {
        byte flag = U8();
        return flag switch
        {
            0 => false,
            1 => true,
            _ => throw new ExspeedProtocolException($"invalid option flag {flag}"),
        };
    }

    public string? OptStr() => Flag() ? Str() : null;

    public byte[]? OptBytes() => Flag() ? Bytes().ToArray() : null;

    public ulong? OptU64() => Flag() ? U64() : null;

    /// <summary>
    /// A u32 element count, rejected when the rest of the payload cannot hold that many items of
    /// <paramref name="minSize"/> bytes (so a hostile count cannot exhaust memory).
    /// </summary>
    public int Count(int minSize)
    {
        uint n = U32();
        if ((ulong)n * (ulong)Math.Max(1, minSize) > (ulong)Remaining)
        {
            throw new ExspeedProtocolException($"count {n} exceeds payload size");
        }
        return (int)n;
    }

    public IReadOnlyList<KeyValuePair<string, string>> Headers()
    {
        int n = U16();
        if (n * 4 > Remaining)
        {
            throw new ExspeedProtocolException("header count exceeds payload");
        }
        if (n == 0)
        {
            return Array.Empty<KeyValuePair<string, string>>();
        }
        var list = new KeyValuePair<string, string>[n];
        for (int i = 0; i < n; i++)
        {
            string k = Str();
            string v = Str();
            list[i] = new KeyValuePair<string, string>(k, v);
        }
        return list;
    }
}
