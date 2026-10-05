using System.Globalization;
using System.Text;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>Header names with a meaning to the server.</summary>
public static class HeaderNames
{
    /// <summary>Expire the record this long after the append (needs <see cref="StreamSpec.AllowMsgTtl"/>).</summary>
    public const string Ttl = "exspeed-ttl";

    /// <summary>Deliver to consumers no earlier than this long after the append (needs <see cref="StreamSpec.AllowDelayed"/>).</summary>
    public const string Delay = "exspeed-delay";

    /// <summary>Deliver to consumers no earlier than this time, ms since the epoch (needs <see cref="StreamSpec.AllowDelayed"/>).</summary>
    public const string DeliverAt = "exspeed-deliver-at";

    /// <summary>0 (default) to 9, higher first, for consumers with a priority window.</summary>
    public const string Priority = "exspeed-priority";

    /// <summary>Marks a key-value tombstone: <c>DEL</c> or <c>PURGE</c>.</summary>
    public const string KvOp = "exspeed-kv-op";
}

/// <summary>
/// A record to publish. The value is raw bytes; use the <see cref="PublishRecord(string, string)"/> constructor
/// for UTF-8 text or <see cref="Json{T}"/> for a JSON-encoded object.
/// </summary>
/// <param name="Subject">The record's subject (dot-separated tokens, e.g. <c>orders.placed</c>).</param>
/// <param name="Value">The payload.</param>
public sealed record PublishRecord(string Subject, ReadOnlyMemory<byte> Value)
{
    /// <summary>A record whose value is <paramref name="value"/> as UTF-8.</summary>
    /// <param name="subject">The record's subject.</param>
    /// <param name="value">The payload text.</param>
    public PublishRecord(string subject, string value) : this(subject, Encoding.UTF8.GetBytes(value)) { }

    /// <summary>A record whose value is <paramref name="value"/> serialized as JSON.</summary>
    /// <typeparam name="T">The value's type.</typeparam>
    /// <param name="subject">The record's subject.</param>
    /// <param name="value">The value to serialize.</param>
    /// <param name="options">JSON serializer options (defaults to <see cref="JsonSerializerOptions.Default"/>).</param>
    /// <returns>The record.</returns>
    public static PublishRecord Json<T>(string subject, T value, JsonSerializerOptions? options = null) =>
        new(subject, JsonSerializer.SerializeToUtf8Bytes(value, options));

    /// <summary>Partition/compaction key.</summary>
    public byte[]? Key { get; init; }

    /// <summary>Record headers, in order (a name may repeat).</summary>
    public IReadOnlyList<KeyValuePair<string, string>>? Headers { get; init; }

    /// <summary>
    /// Idempotency key: a retry with the same <c>MsgId</c> and body returns the original offset with
    /// <see cref="PublishResult.Duplicate"/> set instead of writing again. See <see cref="Exspeed.MsgId.New"/>.
    /// </summary>
    public string? MsgId { get; init; }

    /// <summary>
    /// Expire the record this long after it is appended (sent as <c>exspeed-ttl: &lt;ms&gt;ms</c>, at least 1 ms).
    /// The stream needs <see cref="StreamSpec.AllowMsgTtl"/>.
    /// </summary>
    public TimeSpan? Ttl { get; init; }

    /// <summary>
    /// Deliver to consumers no earlier than this long after the append (header <c>exspeed-delay</c>).
    /// The stream needs <see cref="StreamSpec.AllowDelayed"/>.
    /// </summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>
    /// Deliver to consumers no earlier than this time (header <c>exspeed-deliver-at</c>, ms since the epoch).
    /// The stream needs <see cref="StreamSpec.AllowDelayed"/>.
    /// </summary>
    public DateTimeOffset? DeliverAt { get; init; }

    /// <summary>0 (default) to 9, higher first, for consumers with a priority window (header <c>exspeed-priority</c>).</summary>
    public int? Priority { get; init; }

    internal WirePublishRecord ToWire()
    {
        if (Subject is null)
        {
            throw new ExspeedException("record subject is required");
        }
        var headers = new List<KeyValuePair<string, string>>();
        if (Headers is not null)
        {
            headers.AddRange(Headers);
        }
        if (Ttl is { } ttl)
        {
            headers.Add(new(HeaderNames.Ttl, $"{Math.Max(1, Durations.Millis(ttl, "ttl"))}ms"));
        }
        if (Delay is { } delay)
        {
            headers.Add(new(HeaderNames.Delay, $"{Durations.Millis(delay, "delay")}ms"));
        }
        if (DeliverAt is { } at)
        {
            long ms = at.ToUnixTimeMilliseconds();
            if (ms < 0)
            {
                throw new ExspeedException($"invalid deliverAt: {at}");
            }
            headers.Add(new(HeaderNames.DeliverAt, ms.ToString(CultureInfo.InvariantCulture)));
        }
        if (Priority is { } p)
        {
            if (p is < 0 or > 9)
            {
                throw new ExspeedException($"priority must be an integer from 0 to 9, got {p}");
            }
            headers.Add(new(HeaderNames.Priority, p.ToString(CultureInfo.InvariantCulture)));
        }
        return new WirePublishRecord(Subject, Key, Value, headers, MsgId);
    }
}

/// <summary>The result of publishing one record.</summary>
/// <param name="Offset">The record's offset in the stream.</param>
/// <param name="Duplicate">True when <see cref="PublishRecord.MsgId"/> matched an earlier publish; nothing was written.</param>
public readonly record struct PublishResult(ulong Offset, bool Duplicate);

/// <summary>Generates idempotency keys.</summary>
public static class MsgId
{
    /// <summary>
    /// A new time-ordered UUIDv7 string (<c>xxxxxxxx-xxxx-7xxx-[89ab]xxx-xxxxxxxxxxxx</c>): ids generated later
    /// sort after earlier ones.
    /// </summary>
    /// <returns>The id.</returns>
    public static string New()
    {
        Span<byte> b = stackalloc byte[16];
        System.Security.Cryptography.RandomNumberGenerator.Fill(b[6..]);
        long ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        b[0] = (byte)(ms >> 40);
        b[1] = (byte)(ms >> 32);
        b[2] = (byte)(ms >> 24);
        b[3] = (byte)(ms >> 16);
        b[4] = (byte)(ms >> 8);
        b[5] = (byte)ms;
        b[6] = (byte)(0x70 | (b[6] & 0x0F));
        b[8] = (byte)(0x80 | (b[8] & 0x3F));
        string hex = Convert.ToHexString(b).ToLowerInvariant();
        return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
    }
}
