using System.Text;
using System.Text.Json;
using Exspeed.Protocol;

namespace Exspeed;

/// <summary>A record read from a stream.</summary>
public class StreamRecord
{
    internal StreamRecord(WireRecord r)
    {
        Offset = r.Offset;
        TimestampNs = r.TimestampNs;
        Subject = r.Subject;
        Key = r.Key is null ? default(ReadOnlyMemory<byte>?) : new ReadOnlyMemory<byte>(r.Key);
        Value = r.Value;
        Headers = r.Headers;
    }

    /// <summary>The record's offset in its stream.</summary>
    public ulong Offset { get; }

    /// <summary>Append time, nanoseconds since the Unix epoch (full precision).</summary>
    public ulong TimestampNs { get; }

    /// <summary>Append time.</summary>
    public DateTimeOffset Timestamp => DateTimeOffset.UnixEpoch.AddTicks((long)(TimestampNs / 100));

    /// <summary>The record's subject.</summary>
    public string Subject { get; }

    /// <summary>The record's key, if it has one.</summary>
    public ReadOnlyMemory<byte>? Key { get; }

    /// <summary>The payload.</summary>
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>Headers in wire order; a name can repeat. See <see cref="Header"/>.</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Headers { get; }

    /// <summary>The value as UTF-8 text.</summary>
    /// <returns>The text.</returns>
    public string Text() => Encoding.UTF8.GetString(Value.Span);

    /// <summary>The key as UTF-8 text, or <c>null</c> without a key.</summary>
    /// <returns>The key text.</returns>
    public string? KeyText() => Key is { } k ? Encoding.UTF8.GetString(k.Span) : null;

    /// <summary>The value parsed as JSON.</summary>
    /// <typeparam name="T">The type to deserialize.</typeparam>
    /// <param name="options">Serializer options (defaults to <see cref="JsonSerializerOptions.Default"/>).</param>
    /// <returns>The deserialized value.</returns>
    public T? Json<T>(JsonSerializerOptions? options = null) => JsonSerializer.Deserialize<T>(Value.Span, options);

    /// <summary>The first header named <paramref name="name"/>, or <c>null</c>.</summary>
    /// <param name="name">The header name.</param>
    /// <returns>Its value.</returns>
    public string? Header(string name)
    {
        foreach (var (k, v) in Headers)
        {
            if (k == name)
            {
                return v;
            }
        }
        return null;
    }
}

/// <summary>What a message needs from the client to settle itself.</summary>
internal interface IMessageSettler
{
    void AckNoWait(string consumer, ulong offset);

    Task NackAsync(string consumer, ulong offset, TimeSpan? delay, CancellationToken cancellationToken);

    Task TermAsync(string consumer, ulong offset, string reason, CancellationToken cancellationToken);

    Task InProgressAsync(string consumer, IReadOnlyList<ulong> offsets, CancellationToken cancellationToken);
}

/// <summary>A record delivered by a consumer (push or pull), with the means to settle it.</summary>
public sealed class Message : StreamRecord
{
    private readonly IMessageSettler _settler;

    internal Message(WireRecord r, string consumer, IMessageSettler settler) : base(r)
    {
        DeliveryCount = r.DeliveryCount;
        Consumer = consumer;
        _settler = settler;
    }

    /// <summary>1 on first delivery, incremented on each redelivery.</summary>
    public int DeliveryCount { get; }

    /// <summary>The consumer that delivered it.</summary>
    public string Consumer { get; }

    /// <summary>
    /// Acknowledge, fire-and-forget: no round trip. Acks made close together are sent as one frame. If the
    /// connection is down the ack is dropped and the record will be redelivered. A failure reported by the
    /// server raises <see cref="ExspeedClient.AsyncError"/>. Use <see cref="ExspeedClient.AckAsync"/> to wait for
    /// confirmation.
    /// </summary>
    public void Ack() => _settler.AckNoWait(Consumer, Offset);

    /// <summary>Ask for redelivery after <paramref name="delay"/> (default: the consumer's backoff).</summary>
    /// <param name="delay">How long to wait before redelivering; <c>null</c> or zero = the consumer's backoff.</param>
    /// <param name="cancellationToken">Cancels waiting for the reply.</param>
    /// <returns>A task that completes when the server confirmed.</returns>
    public Task NackAsync(TimeSpan? delay = null, CancellationToken cancellationToken = default) =>
        _settler.NackAsync(Consumer, Offset, delay, cancellationToken);

    /// <summary>Never redeliver: dead-letter now (to the consumer's DLQ stream, if set).</summary>
    /// <param name="reason">Free text stored in the <c>exspeed-dlq-reason</c> header.</param>
    /// <param name="cancellationToken">Cancels waiting for the reply.</param>
    /// <returns>A task that completes when the server confirmed.</returns>
    public Task TermAsync(string reason = "", CancellationToken cancellationToken = default) =>
        _settler.TermAsync(Consumer, Offset, reason, cancellationToken);

    /// <summary>Still working on it: reset the ack deadline.</summary>
    /// <param name="cancellationToken">Cancels waiting for the reply.</param>
    /// <returns>A task that completes when the server confirmed.</returns>
    public Task InProgressAsync(CancellationToken cancellationToken = default) =>
        _settler.InProgressAsync(Consumer, new[] { Offset }, cancellationToken);
}

/// <summary>Options for <see cref="ExspeedClient.ReadAsync"/>.</summary>
public sealed record ReadOptions
{
    /// <summary>First offset to read. Default 0.</summary>
    public ulong From { get; init; }

    /// <summary>Most records to return. Default 100 (the server caps it at 10 000).</summary>
    public uint MaxRecords { get; init; } = 100;

    /// <summary>Byte budget per response; 0 = server default (1 MiB).</summary>
    public uint MaxBytes { get; init; }

    /// <summary>Long-poll: when caught up, wait up to this long for new records. Default zero.</summary>
    public TimeSpan Wait { get; init; }

    /// <summary>NATS-style subject filter (<c>orders.*</c>, <c>orders.&gt;</c>). Default: all.</summary>
    public string Filter { get; init; } = "";
}

/// <summary>The result of a stateless <see cref="ExspeedClient.ReadAsync"/>.</summary>
/// <param name="Records">The records read.</param>
/// <param name="NextOffset">Pass as <see cref="ReadOptions.From"/> to continue.</param>
/// <param name="HighWatermark">The stream's next offset at the time of the read.</param>
public sealed record ReadResult(IReadOnlyList<StreamRecord> Records, ulong NextOffset, ulong HighWatermark);

/// <summary>From the server's handshake reply.</summary>
/// <param name="ServerVersion">The server's version.</param>
/// <param name="NodeId">The node's id.</param>
/// <param name="Leader">The leader's client address when the connected node is not the leader.</param>
public sealed record ServerInfo(string ServerVersion, string NodeId, string? Leader);

/// <summary>Node id, leadership and server version, from <see cref="ExspeedClient.MetadataAsync"/>.</summary>
/// <param name="NodeId">The node's id.</param>
/// <param name="IsLeader">Whether this node is the leader.</param>
/// <param name="Leader">The leader's client address, when known and not this node.</param>
/// <param name="ServerVersion">The server's version.</param>
public sealed record ServerMetadata(string NodeId, bool IsLeader, string? Leader, string ServerVersion);

/// <summary>The result of a bounded ExQL query.</summary>
public sealed record QueryResult
{
    /// <summary>Column names.</summary>
    public required IReadOnlyList<string> Columns { get; init; }

    /// <summary>Rows, each a list of JSON values in column order.</summary>
    public required IReadOnlyList<IReadOnlyList<JsonElement>> Rows { get; init; }

    /// <summary>Number of rows.</summary>
    public ulong RowCount { get; init; }

    /// <summary>Server-side execution time in milliseconds.</summary>
    public double ExecutionTimeMs { get; init; }

    /// <summary>True when the server's row cap cut the result short.</summary>
    public bool Truncated { get; init; }
}

/// <summary>Why a subscription ended.</summary>
/// <param name="Code">
/// <c>404</c>: the consumer or its stream was deleted. <c>503</c>: the node lost leadership, or the connection was
/// lost and not re-established. <c>0</c>: ended locally (unsubscribe, or the client closed). Other codes come from a
/// failed re-subscribe after a reconnect.
/// </param>
/// <param name="Message">A description.</param>
public sealed record EndReason(int Code, string Message);

/// <summary>Event data carrying an exception.</summary>
public sealed class ExspeedErrorEventArgs : EventArgs
{
    /// <summary>Creates the event data.</summary>
    /// <param name="error">The exception, if any.</param>
    public ExspeedErrorEventArgs(Exception? error)
    {
        Error = error;
    }

    /// <summary>The exception, or <c>null</c> (a <see cref="ExspeedClient.Closed"/> event after a normal close).</summary>
    public Exception? Error { get; }
}

/// <summary>Event data for <see cref="ExspeedClient.Reconnected"/>.</summary>
public sealed class ExspeedReconnectedEventArgs : EventArgs
{
    /// <summary>Creates the event data.</summary>
    /// <param name="serverInfo">The new connection's handshake info.</param>
    public ExspeedReconnectedEventArgs(ServerInfo serverInfo)
    {
        ServerInfo = serverInfo;
    }

    /// <summary>The new connection's handshake info.</summary>
    public ServerInfo ServerInfo { get; }
}
