using System.Text.Json;

namespace Exspeed.Protocol;

/// <summary><c>PublishRecord</c>: str subject, opt&lt;bytes&gt; key, bytes value, headers, opt&lt;str&gt; msg_id.</summary>
internal sealed record WirePublishRecord(
    string Subject,
    byte[]? Key,
    ReadOnlyMemory<byte> Value,
    IReadOnlyList<KeyValuePair<string, string>> Headers,
    string? MsgId)
{
    public void Encode(WireWriter w)
    {
        w.Str(Subject);
        w.OptBytes(Key);
        w.Bytes(Value.Span);
        w.Headers(Headers);
        w.OptStr(MsgId);
    }

    public static WirePublishRecord Decode(WireReader r)
    {
        string subject = r.Str();
        var key = r.OptBytes();
        var value = r.Bytes();
        var headers = r.Headers();
        string? msgId = r.OptStr();
        return new WirePublishRecord(subject, key, value, headers, msgId);
    }

    /// <summary>Approximate encoded size, for batch budgeting.</summary>
    public int ApproxSize()
    {
        int n = 64 + Subject.Length * 3 + Value.Length + (Key?.Length ?? 0) + (MsgId?.Length ?? 0) * 3;
        foreach (var (k, v) in Headers)
        {
            n += 4 + (k.Length + v.Length) * 3;
        }
        return n;
    }
}

/// <summary>
/// <c>WireRecord</c>: u32 len (bytes after this field), u32 crc (CRC32C of the bytes after delivery_count),
/// u16 delivery_count, u64 offset, u64 timestamp_ns, str subject, opt&lt;bytes&gt; key, bytes value, headers.
/// This is also how the server stores records on disk.
/// </summary>
internal sealed record WireRecord(
    ulong Offset,
    ulong TimestampNs,
    ushort DeliveryCount,
    string Subject,
    byte[]? Key,
    ReadOnlyMemory<byte> Value,
    IReadOnlyList<KeyValuePair<string, string>> Headers)
{
    /// <summary>Size of the smallest valid record.</summary>
    public const int MinRecordLen = 35;

    private const int CrcStart = 10;

    /// <summary>Encode one record (length, CRC, delivery count, fields).</summary>
    public byte[] Encode()
    {
        var body = new WireWriter();
        body.U64(Offset);
        body.U64(TimestampNs);
        body.Str(Subject);
        body.OptBytes(Key);
        body.Bytes(Value.Span);
        body.Headers(Headers);
        var b = body.WrittenSpan;
        var w = new WireWriter(CrcStart + b.Length);
        w.U32((uint)(CrcStart - 4 + b.Length));
        w.U32(Crc32C.Compute(b));
        w.U16(DeliveryCount);
        w.Raw(b);
        return w.ToArray();
    }

    /// <summary>Whether a complete encoded record's CRC matches its contents.</summary>
    public static bool VerifyCrc(ReadOnlySpan<byte> record)
    {
        if (record.Length < MinRecordLen)
        {
            return false;
        }
        uint stored = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(record[4..]);
        return stored == Crc32C.Compute(record[CrcStart..]);
    }

    /// <summary>Decode one record, verifying its length field, structure and CRC.</summary>
    public static WireRecord Decode(WireReader r)
    {
        uint len = r.U32();
        if ((ulong)len + 4 < MinRecordLen)
        {
            throw new ExspeedProtocolException($"record length {(ulong)len + 4} too small");
        }
        if (len > int.MaxValue)
        {
            throw new ExspeedProtocolException($"record length {len} too large");
        }
        var rec = r.Raw((int)len);
        // `rec` starts after the length field: crc at 0..4, delivery_count at 4..6, CRC covers 6..
        uint crc = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(rec.Span);
        if (crc != Crc32C.Compute(rec.Span[6..]))
        {
            throw new ExspeedProtocolException("record: CRC mismatch");
        }
        var rr = new WireReader(rec);
        rr.U32();
        ushort deliveryCount = rr.U16();
        ulong offset = rr.U64();
        ulong ts = rr.U64();
        string subject = rr.Str();
        var key = rr.OptBytes();
        var value = rr.Bytes();
        var headers = rr.Headers();
        rr.Finish();
        return new WireRecord(offset, ts, deliveryCount, subject, key, value, headers);
    }

    public static void EncodeList(WireWriter w, IReadOnlyList<WireRecord> records)
    {
        w.U32((uint)records.Count);
        foreach (var rec in records)
        {
            w.Raw(rec.Encode());
        }
    }

    public static IReadOnlyList<WireRecord> DecodeList(WireReader r)
    {
        int n = r.Count(MinRecordLen);
        var list = new WireRecord[n];
        for (int i = 0; i < n; i++)
        {
            list[i] = Decode(r);
        }
        return list;
    }
}

/// <summary>
/// <c>StreamLimits</c> (<c>crates/exspeed-common/src/limits.rs</c>) exactly as serde serializes it:
/// snake_case keys, in declaration order.
/// </summary>
internal sealed record WireStreamLimits(
    ulong MaxMsgs,
    string Discard,
    ulong MaxMsgsPerSubject,
    bool AllowMsgTtl,
    ulong MsgTtlMs,
    bool AllowDelayed,
    string Retention,
    IReadOnlyList<string>? CaptureSubjects = null)
{
    public static readonly WireStreamLimits Default = new(0, "old", 0, false, 0, false, "limits");

    public bool IsDefault =>
        MaxMsgs == 0 && Discard == "old" && MaxMsgsPerSubject == 0 && !AllowMsgTtl && MsgTtlMs == 0
        && !AllowDelayed && Retention == "limits" && CaptureSubjects is not { Count: > 0 };

    public byte[] ToJson()
    {
        using var ms = new MemoryStream();
        using (var j = new Utf8JsonWriter(ms, WireJson.WriterOptions))
        {
            j.WriteStartObject();
            j.WriteNumber("max_msgs", MaxMsgs);
            j.WriteString("discard", Discard);
            j.WriteNumber("max_msgs_per_subject", MaxMsgsPerSubject);
            j.WriteBoolean("allow_msg_ttl", AllowMsgTtl);
            j.WriteNumber("msg_ttl_ms", MsgTtlMs);
            j.WriteBoolean("allow_delayed", AllowDelayed);
            j.WriteString("retention", Retention);
            if (CaptureSubjects is { Count: > 0 })
            {
                j.WriteStartArray("capture_subjects");
                foreach (var s in CaptureSubjects)
                {
                    j.WriteStringValue(s);
                }
                j.WriteEndArray();
            }
            j.WriteEndObject();
        }
        return ms.ToArray();
    }

    public static WireStreamLimits FromJson(ReadOnlyMemory<byte> json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var e = doc.RootElement;
            return new WireStreamLimits(
                WireJson.U64(e, "max_msgs"),
                WireJson.Str(e, "discard") ?? "old",
                WireJson.U64(e, "max_msgs_per_subject"),
                WireJson.Bool(e, "allow_msg_ttl"),
                WireJson.U64(e, "msg_ttl_ms"),
                WireJson.Bool(e, "allow_delayed"),
                WireJson.Str(e, "retention") ?? "limits",
                WireJson.StrList(e, "capture_subjects"));
        }
        catch (JsonException ex)
        {
            throw new ExspeedProtocolException($"invalid stream limits: {ex.Message}");
        }
    }
}

/// <summary>
/// <c>StreamSpec</c>: str name, u64 max_age_secs, u64 max_bytes, u64 dedup_window_secs, u64 dedup_max_entries,
/// u8 compaction, then <c>bytes(JSON)</c> of the limits only when they are not all defaults.
/// </summary>
internal sealed record WireStreamSpec(
    string Name,
    ulong MaxAgeSecs,
    ulong MaxBytes,
    ulong DedupWindowSecs,
    ulong DedupMaxEntries,
    bool Compaction,
    WireStreamLimits? Limits)
{
    public void Encode(WireWriter w)
    {
        w.Str(Name);
        w.U64(MaxAgeSecs);
        w.U64(MaxBytes);
        w.U64(DedupWindowSecs);
        w.U64(DedupMaxEntries);
        w.Bool(Compaction);
        if (Limits is { IsDefault: false })
        {
            w.Bytes(Limits.ToJson());
        }
    }

    public static WireStreamSpec Decode(WireReader r)
    {
        string name = r.Str();
        ulong maxAge = r.U64();
        ulong maxBytes = r.U64();
        ulong dedupWindow = r.U64();
        ulong dedupMax = r.U64();
        bool compaction = r.Bool();
        WireStreamLimits? limits = r.Remaining > 0 ? WireStreamLimits.FromJson(r.Bytes()) : null;
        return new WireStreamSpec(name, maxAge, maxBytes, dedupWindow, dedupMax, compaction, limits);
    }
}

/// <summary>Helpers for the JSON parts of the protocol.</summary>
internal static class WireJson
{
    public static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    public static ulong U64(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number
            ? (v.TryGetUInt64(out var u) ? u : (ulong)Math.Max(0, v.GetDouble()))
            : 0;

    public static bool Bool(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.True;

    public static string? Str(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;

    public static IReadOnlyList<string>? StrList(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Array
            ? v.EnumerateArray().Select(x => x.GetString() ?? "").ToList()
            : null;

    public static bool Has(JsonElement e, string name) =>
        e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind != JsonValueKind.Null;
}
