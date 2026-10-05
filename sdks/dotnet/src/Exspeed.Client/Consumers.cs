using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>Whether delivered records must be acknowledged.</summary>
public enum AckPolicy
{
    /// <summary>Each record must be acked; unacked records are redelivered after the ack wait.</summary>
    Explicit,

    /// <summary>Records count as acked when delivered (at-most-once).</summary>
    None,
}

/// <summary>How <see cref="ConsumerSpec.FilterHeaders"/> combines its entries.</summary>
public enum HeaderMatch
{
    /// <summary>Every header must match.</summary>
    All,

    /// <summary>At least one must match.</summary>
    Any,
}

/// <summary>The kind of a <see cref="DeliverPolicy"/>.</summary>
public enum DeliverPolicyKind
{
    /// <summary>From the first retained record.</summary>
    All,

    /// <summary>Only records appended after the consumer is created.</summary>
    New,

    /// <summary>From a specific offset.</summary>
    FromOffset,

    /// <summary>From the first record at or after a time.</summary>
    FromTime,
}

/// <summary>Where a new consumer starts.</summary>
public sealed record DeliverPolicy
{
    private DeliverPolicy(DeliverPolicyKind kind, ulong value)
    {
        Kind = kind;
        Value = value;
    }

    /// <summary>The kind of policy.</summary>
    public DeliverPolicyKind Kind { get; }

    /// <summary>The offset (<see cref="DeliverPolicyKind.FromOffset"/>) or ms since the epoch (<see cref="DeliverPolicyKind.FromTime"/>).</summary>
    public ulong Value { get; }

    /// <summary>From the first retained record.</summary>
    public static DeliverPolicy All { get; } = new(DeliverPolicyKind.All, 0);

    /// <summary>Only records appended after the consumer is created.</summary>
    public static DeliverPolicy New { get; } = new(DeliverPolicyKind.New, 0);

    /// <summary>From a specific offset.</summary>
    /// <param name="offset">The first offset to deliver.</param>
    /// <returns>The policy.</returns>
    public static DeliverPolicy FromOffset(ulong offset) => new(DeliverPolicyKind.FromOffset, offset);

    /// <summary>From the first record at or after <paramref name="time"/>.</summary>
    /// <param name="time">The start time.</param>
    /// <returns>The policy.</returns>
    public static DeliverPolicy FromTime(DateTimeOffset time) =>
        new(DeliverPolicyKind.FromTime, (ulong)Math.Max(0, time.ToUnixTimeMilliseconds()));

    /// <summary>From the first record at or after <paramref name="unixMs"/> (ms since the epoch).</summary>
    /// <param name="unixMs">The start time in ms since the epoch.</param>
    /// <returns>The policy.</returns>
    public static DeliverPolicy FromTimeMs(ulong unixMs) => new(DeliverPolicyKind.FromTime, unixMs);

    internal void Write(Utf8JsonWriter j)
    {
        switch (Kind)
        {
            case DeliverPolicyKind.All:
                j.WriteStringValue("all");
                break;
            case DeliverPolicyKind.New:
                j.WriteStringValue("new");
                break;
            case DeliverPolicyKind.FromOffset:
                j.WriteStartObject();
                j.WriteNumber("from_offset", Value);
                j.WriteEndObject();
                break;
            default:
                j.WriteStartObject();
                j.WriteNumber("from_time", Value);
                j.WriteEndObject();
                break;
        }
    }

    internal static DeliverPolicy Parse(JsonElement e)
    {
        if (e.ValueKind == JsonValueKind.String)
        {
            return e.GetString() == "new" ? New : All;
        }
        if (e.ValueKind == JsonValueKind.Object)
        {
            if (WireJson.Has(e, "from_offset"))
            {
                return FromOffset(WireJson.U64(e, "from_offset"));
            }
            if (WireJson.Has(e, "from_time"))
            {
                return FromTimeMs(WireJson.U64(e, "from_time"));
            }
        }
        return All;
    }

    /// <inheritdoc />
    public override string ToString() => Kind switch
    {
        DeliverPolicyKind.All => "all",
        DeliverPolicyKind.New => "new",
        DeliverPolicyKind.FromOffset => $"from_offset {Value}",
        _ => $"from_time {Value}",
    };
}

/// <summary>
/// A durable (or ephemeral) consumer: a cursor plus delivery state over one stream. Only <paramref name="Name"/>
/// and <paramref name="Stream"/> are required; properties left <c>null</c> take the server's defaults.
/// </summary>
/// <param name="Name">The consumer name.</param>
/// <param name="Stream">The stream it reads.</param>
public sealed record ConsumerSpec(string Name, string Stream)
{
    /// <summary>Subject filters (<c>orders.placed</c>, <c>orders.eu.&gt;</c>). Empty = all subjects.</summary>
    public IReadOnlyList<string>? FilterSubjects { get; init; }

    /// <summary>Where the consumer starts. Server default <see cref="DeliverPolicy.All"/>.</summary>
    public DeliverPolicy? Deliver { get; init; }

    /// <summary>Server default <see cref="AckPolicy.Explicit"/>.</summary>
    public AckPolicy? Ack { get; init; }

    /// <summary>Redeliver a record not acked within this time (whole ms). Server default 30 s.</summary>
    public TimeSpan? AckWait { get; init; }

    /// <summary>Dead-letter after this many deliveries; 0 = never. Server default 5.</summary>
    public uint? MaxDeliver { get; init; }

    /// <summary>Redelivery delays by delivery count (the last repeats). Empty = redeliver immediately.</summary>
    public IReadOnlyList<TimeSpan>? Backoff { get; init; }

    /// <summary>Pause delivery while this many records await an ack. Server default 1000.</summary>
    public uint? MaxAckPending { get; init; }

    /// <summary>Stream that receives dead letters. Unset = they are dropped (and counted).</summary>
    public string? DlqStream { get; init; }

    /// <summary>Deleted automatically when the connection that created it closes.</summary>
    public bool? Ephemeral { get; init; }

    /// <summary>
    /// Dead-letter records whose TTL expires before they are acked (to <see cref="DlqStream"/>, cause
    /// <c>expired</c>) instead of dropping them silently.
    /// </summary>
    public bool? DeadLetterExpired { get; init; }

    /// <summary>Only records whose headers have these exact values, combined by <see cref="HeaderMatch"/>.</summary>
    public IReadOnlyDictionary<string, string>? FilterHeaders { get; init; }

    /// <summary>How <see cref="FilterHeaders"/> entries combine. Server default <see cref="Exspeed.HeaderMatch.All"/>.</summary>
    public HeaderMatch? HeaderMatch { get; init; }

    /// <summary>
    /// Deliver to one subscription at a time (the oldest connected); the next takes over when it goes away.
    /// Pulls are refused.
    /// </summary>
    public bool? SingleActive { get; init; }

    /// <summary>
    /// Look this many records ahead and deliver higher <see cref="PublishRecord.Priority"/> first
    /// (0 = strictly in order). Server maximum 10 000.
    /// </summary>
    public uint? PriorityWindow { get; init; }

    /// <summary>
    /// The snake_case JSON the server expects, keys in the same order as the Rust <c>ConsumerSpec</c> (so a
    /// fully specified spec serializes byte for byte like serde_json). Unset fields are left out.
    /// </summary>
    internal byte[] ToWireJson()
    {
        if (string.IsNullOrEmpty(Name))
        {
            throw new ExspeedException("consumer name is required");
        }
        if (string.IsNullOrEmpty(Stream))
        {
            throw new ExspeedException("consumer stream is required");
        }
        using var ms = new MemoryStream();
        using (var j = new Utf8JsonWriter(ms, WireJson.WriterOptions))
        {
            j.WriteStartObject();
            j.WriteString("name", Name);
            j.WriteString("stream", Stream);
            if (FilterSubjects is not null)
            {
                j.WriteStartArray("filter_subjects");
                foreach (var s in FilterSubjects)
                {
                    j.WriteStringValue(s);
                }
                j.WriteEndArray();
            }
            if (Deliver is not null)
            {
                j.WritePropertyName("deliver");
                Deliver.Write(j);
            }
            if (Ack is { } ack)
            {
                j.WriteString("ack", ack == AckPolicy.None ? "none" : "explicit");
            }
            if (AckWait is { } wait)
            {
                j.WriteNumber("ack_wait_ms", Durations.Millis(wait, "ackWait"));
            }
            if (MaxDeliver is { } md)
            {
                j.WriteNumber("max_deliver", md);
            }
            if (Backoff is not null)
            {
                j.WriteStartArray("backoff_ms");
                foreach (var b in Backoff)
                {
                    j.WriteNumberValue(Durations.Millis(b, "backoff"));
                }
                j.WriteEndArray();
            }
            if (MaxAckPending is { } map)
            {
                j.WriteNumber("max_ack_pending", map);
            }
            if (DlqStream is not null)
            {
                j.WriteString("dlq_stream", DlqStream);
            }
            if (Ephemeral is { } eph)
            {
                j.WriteBoolean("ephemeral", eph);
            }
            if (DeadLetterExpired is { } dle)
            {
                j.WriteBoolean("dead_letter_expired", dle);
            }
            if (FilterHeaders is { Count: > 0 })
            {
                j.WriteStartObject("filter_headers");
                foreach (var (k, v) in FilterHeaders.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                {
                    j.WriteString(k, v);
                }
                j.WriteEndObject();
            }
            if (HeaderMatch is { } hm)
            {
                j.WriteString("header_match", hm == Exspeed.HeaderMatch.Any ? "any" : "all");
            }
            if (SingleActive is { } sa)
            {
                j.WriteBoolean("single_active", sa);
            }
            if (PriorityWindow is { } pw)
            {
                j.WriteNumber("priority_window", pw);
            }
            j.WriteEndObject();
        }
        return ms.ToArray();
    }

    /// <summary>A spec as the server reports it (every field set; missing ones take the server's defaults).</summary>
    internal static ConsumerSpec Parse(JsonElement e)
    {
        var filters = new List<string>();
        if (e.ValueKind == JsonValueKind.Object && e.TryGetProperty("filter_subjects", out var fs) && fs.ValueKind == JsonValueKind.Array)
        {
            foreach (var f in fs.EnumerateArray())
            {
                filters.Add(f.GetString() ?? "");
            }
        }
        var backoff = new List<TimeSpan>();
        if (e.ValueKind == JsonValueKind.Object && e.TryGetProperty("backoff_ms", out var bo) && bo.ValueKind == JsonValueKind.Array)
        {
            foreach (var b in bo.EnumerateArray())
            {
                backoff.Add(TimeSpan.FromMilliseconds(b.GetDouble()));
            }
        }
        var headers = new Dictionary<string, string>();
        if (e.ValueKind == JsonValueKind.Object && e.TryGetProperty("filter_headers", out var fh) && fh.ValueKind == JsonValueKind.Object)
        {
            foreach (var p in fh.EnumerateObject())
            {
                headers[p.Name] = p.Value.ValueKind == JsonValueKind.String ? p.Value.GetString()! : p.Value.ToString();
            }
        }
        return new ConsumerSpec(WireJson.Str(e, "name") ?? "", WireJson.Str(e, "stream") ?? "")
        {
            FilterSubjects = filters,
            Deliver = e.ValueKind == JsonValueKind.Object && e.TryGetProperty("deliver", out var d) ? DeliverPolicy.Parse(d) : DeliverPolicy.All,
            Ack = WireJson.Str(e, "ack") == "none" ? AckPolicy.None : AckPolicy.Explicit,
            AckWait = TimeSpan.FromMilliseconds(WireJson.Has(e, "ack_wait_ms") ? WireJson.U64(e, "ack_wait_ms") : 30_000),
            MaxDeliver = (uint)(WireJson.Has(e, "max_deliver") ? WireJson.U64(e, "max_deliver") : 5),
            Backoff = backoff,
            MaxAckPending = (uint)(WireJson.Has(e, "max_ack_pending") ? WireJson.U64(e, "max_ack_pending") : 1000),
            DlqStream = WireJson.Str(e, "dlq_stream"),
            Ephemeral = WireJson.Bool(e, "ephemeral"),
            DeadLetterExpired = WireJson.Bool(e, "dead_letter_expired"),
            FilterHeaders = headers,
            HeaderMatch = WireJson.Str(e, "header_match") == "any" ? Exspeed.HeaderMatch.Any : Exspeed.HeaderMatch.All,
            SingleActive = WireJson.Bool(e, "single_active"),
            PriorityWindow = (uint)WireJson.U64(e, "priority_window"),
        };
    }
}

/// <summary>A consumer's delivery counters.</summary>
public sealed record ConsumerStats
{
    /// <summary>Records delivered for the first time.</summary>
    public ulong Delivered { get; init; }

    /// <summary>Redeliveries.</summary>
    public ulong Redelivered { get; init; }

    /// <summary>Records acked.</summary>
    public ulong Acked { get; init; }

    /// <summary>Records dead-lettered (or dropped without a DLQ).</summary>
    public ulong DeadLettered { get; init; }

    /// <summary>Unacked records that disappeared before redelivery (retention or compaction removed them).</summary>
    public ulong Gone { get; init; }

    /// <summary>Records skipped because the consumer fell behind retention.</summary>
    public ulong Skipped { get; init; }

    internal static ConsumerStats Parse(JsonElement e) => new()
    {
        Delivered = WireJson.U64(e, "delivered"),
        Redelivered = WireJson.U64(e, "redelivered"),
        Acked = WireJson.U64(e, "acked"),
        DeadLettered = WireJson.U64(e, "dead_lettered"),
        Gone = WireJson.U64(e, "gone"),
        Skipped = WireJson.U64(e, "skipped"),
    };
}

/// <summary>A consumer's spec, position and counters.</summary>
public sealed record ConsumerInfo
{
    /// <summary>The consumer's spec, with the server's defaults filled in.</summary>
    public required ConsumerSpec Spec { get; init; }

    /// <summary>Next stream offset to be delivered for the first time.</summary>
    public ulong NextOffset { get; init; }

    /// <summary>Everything below this offset is acked (or filtered out).</summary>
    public ulong AckFloor { get; init; }

    /// <summary>Records delivered and not yet acked.</summary>
    public ulong NumUnacked { get; init; }

    /// <summary>Records currently out with a subscriber or puller.</summary>
    public ulong NumInFlight { get; init; }

    /// <summary>Records held back until their delivery time (<c>delay</c> / <c>deliverAt</c>).</summary>
    public ulong NumDelayed { get; init; }

    /// <summary>Records not yet delivered (approximate: includes filtered-out ones).</summary>
    public ulong NumWaiting { get; init; }

    /// <summary>High watermark minus the ack floor.</summary>
    public ulong Lag { get; init; }

    /// <summary>Live push subscriptions.</summary>
    public ulong Subscribers { get; init; }

    /// <summary>Pull requests waiting for records.</summary>
    public ulong PullWaiters { get; init; }

    /// <summary>Delivery counters.</summary>
    public required ConsumerStats Stats { get; init; }

    /// <summary>The JSON object as the server sent it (snake_case keys), including fields this type does not model.</summary>
    public JsonElement Raw { get; init; }

    internal static ConsumerInfo Parse(JsonElement e) => new()
    {
        Spec = ConsumerSpec.Parse(e.ValueKind == JsonValueKind.Object && e.TryGetProperty("spec", out var s) ? s : default),
        NextOffset = WireJson.U64(e, "next_offset"),
        AckFloor = WireJson.U64(e, "ack_floor"),
        NumUnacked = WireJson.U64(e, "num_unacked"),
        NumInFlight = WireJson.U64(e, "num_in_flight"),
        NumDelayed = WireJson.U64(e, "num_delayed"),
        NumWaiting = WireJson.U64(e, "num_waiting"),
        Lag = WireJson.U64(e, "lag"),
        Subscribers = WireJson.U64(e, "subscribers"),
        PullWaiters = WireJson.U64(e, "pull_waiters"),
        Stats = ConsumerStats.Parse(e.ValueKind == JsonValueKind.Object && e.TryGetProperty("stats", out var st) ? st : default),
        Raw = e.Clone(),
    };
}

/// <summary>Where to move a consumer's cursor with <see cref="ExspeedClient.SeekAsync"/>.</summary>
public sealed record SeekTarget
{
    private SeekTarget(byte kind, ulong value)
    {
        Kind = kind;
        Value = value;
    }

    internal byte Kind { get; }

    internal ulong Value { get; }

    /// <summary>The first retained record.</summary>
    public static SeekTarget Earliest { get; } = new(0, 0);

    /// <summary>The end of the stream (only new records).</summary>
    public static SeekTarget Latest { get; } = new(1, 0);

    /// <summary>A specific offset.</summary>
    /// <param name="offset">The offset.</param>
    /// <returns>The target.</returns>
    public static SeekTarget Offset(ulong offset) => new(2, offset);

    /// <summary>The first record at or after <paramref name="time"/>.</summary>
    /// <param name="time">The time.</param>
    /// <returns>The target.</returns>
    public static SeekTarget Time(DateTimeOffset time) => new(3, (ulong)Math.Max(0, time.ToUnixTimeMilliseconds()));

    /// <summary>The first record at or after <paramref name="unixMs"/> (ms since the epoch).</summary>
    /// <param name="unixMs">The time in ms since the epoch.</param>
    /// <returns>The target.</returns>
    public static SeekTarget TimeMs(ulong unixMs) => new(3, unixMs);
}

/// <summary>Options for <see cref="ExspeedClient.SubscribeAsync"/>.</summary>
public sealed record SubscribeOptions
{
    /// <summary>
    /// Credit window: how many records the server may push before the application has taken them. The client
    /// returns credit as you take messages. Default 256.
    /// </summary>
    public uint Window { get; init; } = 256;
}

/// <summary>Options for <see cref="ExspeedClient.PullAsync"/>.</summary>
public sealed record PullOptions
{
    /// <summary>Most messages to return. Default 100.</summary>
    public uint MaxMessages { get; init; } = 100;

    /// <summary>Byte budget; 0 = server default (4 MiB).</summary>
    public uint MaxBytes { get; init; }

    /// <summary>Wait up to this long for at least one message. Default 5 s.</summary>
    public TimeSpan Expires { get; init; } = TimeSpan.FromSeconds(5);
}
