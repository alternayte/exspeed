using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>What a stream does when it reaches <see cref="StreamSpec.MaxMsgs"/>.</summary>
public enum DiscardPolicy
{
    /// <summary>Drop the oldest records (the default).</summary>
    Old,

    /// <summary>Reject new records with a 429 error.</summary>
    New,
}

/// <summary>When records leave a stream.</summary>
public enum RetentionPolicy
{
    /// <summary>Records stay until a limit (age, size, count) removes them (the default).</summary>
    Limits,

    /// <summary>At most one consumer; a record is removed once that consumer acked it.</summary>
    WorkQueue,

    /// <summary>A record is removed once every consumer of the stream acked it.</summary>
    Interest,
}

/// <summary>
/// Stream settings for <see cref="ExspeedClient.CreateStreamAsync(StreamSpec, CancellationToken)"/> and
/// <see cref="ExspeedClient.UpdateStreamAsync"/>. Zero (or the default value) means "server default" for every
/// numeric setting.
/// </summary>
/// <param name="Name">The stream name.</param>
public sealed record StreamSpec(string Name)
{
    /// <summary>Retention by age (whole seconds, rounded up); <see cref="TimeSpan.Zero"/> = server default.</summary>
    public TimeSpan MaxAge { get; init; }

    /// <summary>Retention by size in bytes; 0 = server default.</summary>
    public ulong MaxBytes { get; init; }

    /// <summary>How long <c>msg_id</c>s are remembered for deduplication; zero = server default.</summary>
    public TimeSpan DedupWindow { get; init; }

    /// <summary>Most <c>msg_id</c>s remembered; 0 = server default.</summary>
    public ulong DedupMaxEntries { get; init; }

    /// <summary>Keep only the latest record per key (log compaction).</summary>
    public bool Compaction { get; init; }

    /// <summary>Most records the stream holds; 0 = no limit. What happens at the limit is <see cref="Discard"/>.</summary>
    public ulong MaxMsgs { get; init; }

    /// <summary>At <see cref="MaxMsgs"/>: drop the oldest records (default) or reject new ones.</summary>
    public DiscardPolicy Discard { get; init; }

    /// <summary>Most records kept per subject (older ones are removed); 0 = no limit.</summary>
    public ulong MaxMsgsPerSubject { get; init; }

    /// <summary>Accept the per-record <see cref="PublishRecord.Ttl"/> option (header <c>exspeed-ttl</c>).</summary>
    public bool AllowMsgTtl { get; init; }

    /// <summary>Default lifetime of every record (milliseconds); zero = none.</summary>
    public TimeSpan MsgTtl { get; init; }

    /// <summary>Accept the <see cref="PublishRecord.Delay"/> / <see cref="PublishRecord.DeliverAt"/> options.</summary>
    public bool AllowDelayed { get; init; }

    /// <summary>When records leave the stream; see <see cref="RetentionPolicy"/>.</summary>
    public RetentionPolicy Retention { get; init; }

    /// <summary>
    /// Subject filters (<c>orders.&gt;</c>) whose core messages the stream also stores: a core message published
    /// to a matching subject is appended to this stream too. No two streams may capture overlapping subjects.
    /// </summary>
    public IReadOnlyList<string>? CaptureSubjects { get; init; }

    internal WireStreamSpec ToWire()
    {
        if (string.IsNullOrEmpty(Name))
        {
            throw new ExspeedException("stream name is required");
        }
        var limits = new WireStreamLimits(
            MaxMsgs,
            Discard == DiscardPolicy.New ? "new" : "old",
            MaxMsgsPerSubject,
            AllowMsgTtl,
            Durations.Millis(MsgTtl, "msgTtl"),
            AllowDelayed,
            Retention.ToWire(),
            CaptureSubjects is { Count: > 0 } c ? c.ToList() : null);
        return new WireStreamSpec(
            Name,
            Durations.Seconds(MaxAge, "maxAge"),
            MaxBytes,
            Durations.Seconds(DedupWindow, "dedupWindow"),
            DedupMaxEntries,
            Compaction,
            limits.IsDefault ? null : limits);
    }
}

/// <summary>A stream's settings, as reported by the server.</summary>
public sealed record StreamConfig
{
    /// <summary>Retention by age; zero = none.</summary>
    public TimeSpan MaxAge { get; init; }

    /// <summary>Retention by size in bytes; 0 = none.</summary>
    public ulong MaxBytes { get; init; }

    /// <summary>How long <c>msg_id</c>s are remembered.</summary>
    public TimeSpan DedupWindow { get; init; }

    /// <summary>Most <c>msg_id</c>s remembered.</summary>
    public ulong DedupMaxEntries { get; init; }

    /// <summary>Whether the stream is compacted.</summary>
    public bool Compaction { get; init; }

    /// <summary>Most records the stream holds; 0 = no limit.</summary>
    public ulong MaxMsgs { get; init; }

    /// <summary>What happens at <see cref="MaxMsgs"/>.</summary>
    public DiscardPolicy Discard { get; init; }

    /// <summary>Most records kept per subject; 0 = no limit.</summary>
    public ulong MaxMsgsPerSubject { get; init; }

    /// <summary>Whether per-record TTLs are accepted.</summary>
    public bool AllowMsgTtl { get; init; }

    /// <summary>Default lifetime of every record; zero = none.</summary>
    public TimeSpan MsgTtl { get; init; }

    /// <summary>Whether delayed delivery is accepted.</summary>
    public bool AllowDelayed { get; init; }

    /// <summary>The retention policy.</summary>
    public RetentionPolicy Retention { get; init; }

    /// <summary>Subject filters whose core messages the stream also stores (empty = none).</summary>
    public IReadOnlyList<string> CaptureSubjects { get; init; } = Array.Empty<string>();

    internal static StreamConfig Parse(JsonElement e) => new()
    {
        MaxAge = TimeSpan.FromSeconds(WireJson.U64(e, "max_age_secs")),
        MaxBytes = WireJson.U64(e, "max_bytes"),
        DedupWindow = TimeSpan.FromSeconds(WireJson.U64(e, "dedup_window_secs")),
        DedupMaxEntries = WireJson.U64(e, "dedup_max_entries"),
        Compaction = WireJson.Bool(e, "compaction"),
        MaxMsgs = WireJson.U64(e, "max_msgs"),
        Discard = WireJson.Str(e, "discard") == "new" ? DiscardPolicy.New : DiscardPolicy.Old,
        MaxMsgsPerSubject = WireJson.U64(e, "max_msgs_per_subject"),
        AllowMsgTtl = WireJson.Bool(e, "allow_msg_ttl"),
        MsgTtl = TimeSpan.FromMilliseconds(WireJson.U64(e, "msg_ttl_ms")),
        AllowDelayed = WireJson.Bool(e, "allow_delayed"),
        Retention = RetentionPolicies.Parse(WireJson.Str(e, "retention")),
        CaptureSubjects = WireJson.StrList(e, "capture_subjects") ?? Array.Empty<string>(),
    };
}

/// <summary>A stream's offsets and settings.</summary>
public sealed record StreamInfo
{
    /// <summary>The stream name.</summary>
    public required string Name { get; init; }

    /// <summary>Offset of the first retained record.</summary>
    public ulong EarliestOffset { get; init; }

    /// <summary>The offset the next record gets (the high watermark).</summary>
    public ulong NextOffset { get; init; }

    /// <summary>Records between <see cref="EarliestOffset"/> and <see cref="NextOffset"/>.</summary>
    public ulong Records { get; init; }

    /// <summary>The stream's settings.</summary>
    public required StreamConfig Config { get; init; }

    /// <summary>Internal streams start with <c>__</c>.</summary>
    public bool Internal { get; init; }

    /// <summary>The JSON object as the server sent it (snake_case keys), including fields this type does not model.</summary>
    public JsonElement Raw { get; init; }

    internal static StreamInfo Parse(JsonElement e) => new()
    {
        Name = WireJson.Str(e, "name") ?? "",
        EarliestOffset = WireJson.U64(e, "earliest_offset"),
        NextOffset = WireJson.U64(e, "next_offset"),
        Records = WireJson.U64(e, "records"),
        Config = StreamConfig.Parse(e.TryGetProperty("config", out var c) ? c : default),
        Internal = WireJson.Bool(e, "internal"),
        Raw = e.Clone(),
    };
}

internal static class RetentionPolicies
{
    public static string ToWire(this RetentionPolicy p) => p switch
    {
        RetentionPolicy.WorkQueue => "work_queue",
        RetentionPolicy.Interest => "interest",
        _ => "limits",
    };

    public static RetentionPolicy Parse(string? s) => s switch
    {
        "work_queue" => RetentionPolicy.WorkQueue,
        "interest" => RetentionPolicy.Interest,
        _ => RetentionPolicy.Limits,
    };
}

internal static class Durations
{
    /// <summary>Whole milliseconds, rounded up.</summary>
    public static ulong Millis(TimeSpan t, string name)
    {
        if (t < TimeSpan.Zero)
        {
            throw new ExspeedException($"{name} must not be negative, got {t}");
        }
        return (ulong)Math.Ceiling(t.TotalMilliseconds);
    }

    /// <summary>Whole seconds, rounded up.</summary>
    public static ulong Seconds(TimeSpan t, string name)
    {
        if (t < TimeSpan.Zero)
        {
            throw new ExspeedException($"{name} must not be negative, got {t}");
        }
        return (ulong)Math.Ceiling(t.TotalSeconds);
    }

    /// <summary>Milliseconds clamped to a u32 wire field.</summary>
    public static uint Millis32(TimeSpan t, string name)
    {
        ulong ms = Millis(t, name);
        return ms > uint.MaxValue ? uint.MaxValue : (uint)ms;
    }
}
