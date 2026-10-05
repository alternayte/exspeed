using System.Buffers.Binary;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

namespace Exspeed.Protocol;

/// <summary>
/// CRC32C (Castagnoli), as used by the record encoding: every <c>WireRecord</c> carries the CRC32C of its
/// bytes after <c>delivery_count</c>. Uses the SSE4.2 or ARMv8 CRC instructions when available.
/// </summary>
internal static class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
            {
                c = (c & 1) != 0 ? (c >> 1) ^ 0x82F63B78u : c >> 1;
            }
            t[i] = c;
        }
        return t;
    }

    /// <summary>The CRC32C of <paramref name="data"/>.</summary>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        if (Sse42.X64.IsSupported)
        {
            ulong c = crc;
            while (data.Length >= 8)
            {
                c = Sse42.X64.Crc32(c, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }
            crc = (uint)c;
            foreach (byte b in data)
            {
                crc = Sse42.Crc32(crc, b);
            }
        }
        else if (Crc32.Arm64.IsSupported)
        {
            while (data.Length >= 8)
            {
                crc = Crc32.Arm64.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }
            foreach (byte b in data)
            {
                crc = Crc32.ComputeCrc32C(crc, b);
            }
        }
        else
        {
            foreach (byte b in data)
            {
                crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
            }
        }
        return crc ^ 0xFFFFFFFFu;
    }

    /// <summary>The table-driven implementation (used to check the hardware paths in tests).</summary>
    internal static uint ComputeSoftware(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data)
        {
            crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFFu;
    }
}
