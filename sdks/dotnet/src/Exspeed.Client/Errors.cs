using System.Text;
using System.Text.Json;

namespace Exspeed;

/// <summary>Base class of every exception this library throws.</summary>
public class ExspeedException : Exception
{
    /// <summary>Creates an exception with a message.</summary>
    /// <param name="message">What went wrong.</param>
    public ExspeedException(string message) : base(message) { }

    /// <summary>Creates an exception with a message and the exception that caused it.</summary>
    /// <param name="message">What went wrong.</param>
    /// <param name="inner">The underlying exception.</param>
    public ExspeedException(string message, Exception? inner) : base(message, inner) { }
}

/// <summary>
/// The server answered a request with an <c>Error</c> frame.
/// </summary>
/// <remarks>
/// <see cref="Code"/> is HTTP-like (see <see cref="ErrorCodes"/>). <see cref="Detail"/> is the optional
/// machine-readable JSON the server attached, for example <c>{"leader": "host:5933"}</c> (503),
/// <c>{"stored_offset": 7}</c> (409) or <c>{"retry_after_secs": 30}</c> (429). It is passed through as the
/// server sent it, with snake_case keys.
/// </remarks>
public sealed class ExspeedServerException : ExspeedException
{
    /// <summary>Creates a server exception.</summary>
    /// <param name="code">The server's error code.</param>
    /// <param name="message">The server's error message.</param>
    /// <param name="detail">The raw JSON detail, if any.</param>
    public ExspeedServerException(int code, string message, string? detail = null) : base(message)
    {
        Code = code;
        DetailJson = string.IsNullOrEmpty(detail) ? null : detail;
        if (DetailJson is not null)
        {
            try
            {
                using var doc = JsonDocument.Parse(DetailJson);
                Detail = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                Detail = null;
            }
        }
    }

    internal static ExspeedServerException FromWire(ushort code, string message, byte[]? detail)
    {
        string? json = detail is { Length: > 0 } d ? Encoding.UTF8.GetString(d) : null;
        return new ExspeedServerException(code, message, json);
    }

    /// <summary>The HTTP-like error code (400, 401, 403, 404, 409, 429, 500, 503, 507, ...).</summary>
    public int Code { get; }

    /// <summary>The detail JSON as the server sent it, or <c>null</c>.</summary>
    public string? DetailJson { get; }

    /// <summary>The parsed detail JSON, or <c>null</c> when there is none (or it is not valid JSON).</summary>
    public JsonElement? Detail { get; }

    /// <summary>
    /// <c>detail.leader</c> of a 503 "not the leader" error: the leader's client address, when known.
    /// </summary>
    public string? LeaderHint =>
        Detail is { ValueKind: JsonValueKind.Object } d
        && d.TryGetProperty("leader", out var l)
        && l.ValueKind == JsonValueKind.String
            ? l.GetString()
            : null;

    /// <inheritdoc />
    public override string ToString() => $"ExspeedServerException {Code}: {Message}";
}

/// <summary>The connection is closed, was lost, or is being re-established.</summary>
public sealed class ExspeedConnectionException : ExspeedException
{
    /// <summary>Creates a connection exception.</summary>
    /// <param name="message">What went wrong.</param>
    /// <param name="inner">The underlying exception, if any.</param>
    public ExspeedConnectionException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>No response arrived within the request timeout.</summary>
public sealed class ExspeedTimeoutException : ExspeedException
{
    /// <summary>Creates a timeout exception.</summary>
    /// <param name="message">What timed out.</param>
    public ExspeedTimeoutException(string message = "request timed out") : base(message) { }
}

/// <summary>The peer sent bytes this library cannot decode, or a reply of the wrong type.</summary>
public sealed class ExspeedProtocolException : ExspeedException
{
    /// <summary>Creates a protocol exception.</summary>
    /// <param name="message">What could not be decoded.</param>
    public ExspeedProtocolException(string message) : base(message) { }
}

/// <summary>Error codes the server returns (HTTP-like).</summary>
public static class ErrorCodes
{
    /// <summary>Malformed request, invalid name or filter, invalid config.</summary>
    public const int BadRequest = 400;

    /// <summary>Not authenticated (bad or missing token).</summary>
    public const int Unauthorized = 401;

    /// <summary>The credential lacks the needed action on the stream.</summary>
    public const int Forbidden = 403;

    /// <summary>Stream, consumer, bucket or key not found; or a request had no responders.</summary>
    public const int NotFound = 404;

    /// <summary>A bounded query timed out.</summary>
    public const int QueryTimeout = 408;

    /// <summary>
    /// Exists with different settings, stream still has consumers, <c>msg_id</c> reused with a different
    /// body, or a KV key is not at the expected revision.
    /// </summary>
    public const int Conflict = 409;

    /// <summary>A bounded query exceeded the server's query memory limit.</summary>
    public const int QueryTooLarge = 422;

    /// <summary>Retry later (dedup map full, the stream is full, too many concurrent waiting requests).</summary>
    public const int TooManyRequests = 429;

    /// <summary>Internal server error.</summary>
    public const int Internal = 500;

    /// <summary>Not the leader (see <see cref="ExspeedServerException.LeaderHint"/>), or still starting.</summary>
    public const int Unavailable = 503;

    /// <summary>The server's disk is full; nothing was written. Retry once space is freed.</summary>
    public const int InsufficientStorage = 507;
}
