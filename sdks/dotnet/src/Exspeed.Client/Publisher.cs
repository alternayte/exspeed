using Exspeed.Protocol;

namespace Exspeed;

/// <summary>Options for <see cref="ExspeedClient.CreatePublisher"/>.</summary>
public sealed record PublisherOptions
{
    /// <summary>
    /// How long to gather records before sending a batch. Zero (the default) sends whatever was published while
    /// the previous flush was being scheduled, without adding latency.
    /// </summary>
    public TimeSpan BatchWindow { get; init; } = TimeSpan.Zero;

    /// <summary>Most records per batch request. Default 512.</summary>
    public int MaxBatchRecords { get; init; } = 512;

    /// <summary>Records accepted but not yet acknowledged; <see cref="Publisher.PublishAsync"/> waits when full. Default 4096.</summary>
    public int MaxInFlight { get; init; } = 4096;
}

/// <summary>How a publisher sends its requests (the client, or a test double).</summary>
internal interface IPublisherTransport
{
    /// <summary>Queue the request before returning (so wire order matches call order), then await the reply.</summary>
    Task<Response> SendAsync(Request req);
}

/// <summary>
/// A pipelined, coalescing publisher, from <see cref="ExspeedClient.CreatePublisher"/>. Concurrent
/// <see cref="PublishAsync"/> calls are gathered into <c>PublishBatch</c> requests (one per run of records for the
/// same stream) and many batches can be in flight at once. Records reach the stream in the order
/// <see cref="PublishAsync"/> was called, and every call gets its own record's result.
/// </summary>
public sealed class Publisher : IAsyncDisposable
{
    /// <summary>Keep each batch frame well below the 16 MiB frame limit.</summary>
    private const int MaxBatchBytes = 4 * 1024 * 1024;

    private sealed record Queued(string Stream, WirePublishRecord Record, int Size, TaskCompletionSource<PublishResult> Tcs);

    private readonly IPublisherTransport _transport;
    private readonly TimeSpan _batchWindow;
    private readonly int _maxBatchRecords;
    private readonly int _maxInFlight;
    private readonly object _gate = new();
    private readonly List<Queued> _queue = new();
    private readonly Queue<Queued> _blocked = new();
    private readonly List<TaskCompletionSource> _idleWaiters = new();
    private int _inFlight;
    private bool _scheduled;
    private bool _closed;

    internal Publisher(IPublisherTransport transport, PublisherOptions? options)
    {
        options ??= new PublisherOptions();
        _transport = transport;
        _batchWindow = options.BatchWindow < TimeSpan.Zero ? TimeSpan.Zero : options.BatchWindow;
        _maxBatchRecords = Math.Max(1, options.MaxBatchRecords);
        _maxInFlight = Math.Max(1, options.MaxInFlight);
    }

    /// <summary>Records accepted and not yet acknowledged (including those waiting for room).</summary>
    public int Pending
    {
        get
        {
            lock (_gate)
            {
                return _inFlight + _blocked.Count;
            }
        }
    }

    /// <summary>
    /// Publish one record; completes with its offset once the server has it. When <see cref="PublisherOptions.MaxInFlight"/>
    /// records are outstanding, the record waits for room (in call order). Once accepted, a record cannot be
    /// cancelled: <paramref name="cancellationToken"/> only stops waiting for its result.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="record">The record.</param>
    /// <param name="cancellationToken">Stops waiting for the result.</param>
    /// <returns>The record's offset and duplicate flag.</returns>
    public Task<PublishResult> PublishAsync(string stream, PublishRecord record, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<PublishResult>(cancellationToken);
        }
        WirePublishRecord wire;
        try
        {
            wire = record.ToWire();
        }
        catch (Exception e)
        {
            return Task.FromException<PublishResult>(e);
        }
        var tcs = new TaskCompletionSource<PublishResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var q = new Queued(stream, wire, wire.ApproxSize(), tcs);
        lock (_gate)
        {
            if (_closed)
            {
                return Task.FromException<PublishResult>(new ExspeedConnectionException("publisher is closed"));
            }
            if (_inFlight < _maxInFlight && _blocked.Count == 0)
            {
                _inFlight++;
                AddLocked(q);
            }
            else
            {
                // FIFO hand-off keeps call order intact while waiting for capacity.
                _blocked.Enqueue(q);
            }
        }
        return cancellationToken.CanBeCanceled ? tcs.Task.WaitAsync(cancellationToken) : tcs.Task;
    }

    /// <summary>Wait until every accepted record has been acknowledged (or failed).</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>A task that completes when nothing is outstanding.</returns>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        TaskCompletionSource idle;
        lock (_gate)
        {
            if (_queue.Count > 0)
            {
                FlushLocked();
            }
            if (_inFlight == 0 && _blocked.Count == 0)
            {
                return Task.CompletedTask;
            }
            idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _idleWaiters.Add(idle);
        }
        return idle.Task.WaitAsync(cancellationToken);
    }

    /// <summary>Flush, then reject further publishes.</summary>
    /// <returns>A task that completes when everything accepted was acknowledged.</returns>
    public async Task CloseAsync()
    {
        await FlushAsync().ConfigureAwait(false);
        lock (_gate)
        {
            _closed = true;
        }
    }

    /// <summary>Same as <see cref="CloseAsync"/>.</summary>
    /// <returns>A task that completes when closed.</returns>
    public ValueTask DisposeAsync() => new(CloseAsync());

    private void AddLocked(Queued q)
    {
        _queue.Add(q);
        if (_queue.Count >= _maxBatchRecords)
        {
            FlushLocked();
        }
        else
        {
            ScheduleLocked();
        }
    }

    private void ScheduleLocked()
    {
        if (_scheduled)
        {
            return;
        }
        _scheduled = true;
        if (_batchWindow == TimeSpan.Zero)
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ => RunScheduled(), null);
        }
        else
        {
            _ = Task.Delay(_batchWindow).ContinueWith(_ => RunScheduled(), TaskScheduler.Default);
        }
    }

    private void RunScheduled()
    {
        lock (_gate)
        {
            _scheduled = false;
            FlushLocked();
        }
    }

    /// <summary>Send everything queued, as runs of the same stream, in arrival order.</summary>
    private void FlushLocked()
    {
        int i = 0;
        while (i < _queue.Count)
        {
            string stream = _queue[i].Stream;
            var run = new List<Queued>();
            int bytes = 0;
            while (i < _queue.Count
                && _queue[i].Stream == stream
                && run.Count < _maxBatchRecords
                && (run.Count == 0 || bytes + _queue[i].Size <= MaxBatchBytes))
            {
                bytes += _queue[i].Size;
                run.Add(_queue[i]);
                i++;
            }
            SendRun(stream, run);
        }
        _queue.Clear();
    }

    /// <summary>The request is queued synchronously (under the lock), so wire order matches arrival order.</summary>
    private void SendRun(string stream, List<Queued> run)
    {
        bool single = run.Count == 1;
        Request req = single
            ? new Request.Publish(stream, run[0].Record)
            : new Request.PublishBatch(stream, run.Select(q => q.Record).ToList());
        Task<Response> sent;
        try
        {
            sent = _transport.SendAsync(req);
        }
        catch (Exception e)
        {
            sent = Task.FromException<Response>(e);
        }
        // Never inline: completing may hand permits to waiting records, which re-enters the queue.
        sent.ContinueWith(
            t => Complete(req, run, t),
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    private void Complete(Request req, List<Queued> run, Task<Response> t)
    {
        if (t.IsCompletedSuccessfully)
        {
            var resp = t.Result;
            if (run.Count == 1 && resp is Response.PublishOk ok)
            {
                run[0].Tcs.TrySetResult(new PublishResult(ok.Offset, ok.Duplicate));
            }
            else if (resp is Response.PublishBatchOk batch && batch.Results.Count == run.Count)
            {
                for (int i = 0; i < run.Count; i++)
                {
                    run[i].Tcs.TrySetResult(new PublishResult(batch.Results[i].Offset, batch.Results[i].Duplicate));
                }
            }
            else
            {
                var err = new ExspeedProtocolException($"unexpected reply to {req.Name}: {resp.Name}");
                foreach (var q in run)
                {
                    q.Tcs.TrySetException(err);
                }
            }
        }
        else
        {
            Exception err = t.Exception?.InnerException ?? new ExspeedConnectionException("publish cancelled");
            foreach (var q in run)
            {
                q.Tcs.TrySetException(err);
            }
        }
        Release(run.Count);
    }

    private void Release(int n)
    {
        List<TaskCompletionSource>? idle = null;
        lock (_gate)
        {
            for (int i = 0; i < n; i++)
            {
                if (_blocked.Count > 0)
                {
                    // The permit passes straight to the next waiting record.
                    AddLocked(_blocked.Dequeue());
                }
                else
                {
                    _inFlight--;
                }
            }
            if (_inFlight == 0 && _blocked.Count == 0 && _queue.Count == 0)
            {
                idle = _idleWaiters.ToList();
                _idleWaiters.Clear();
            }
        }
        if (idle is not null)
        {
            foreach (var w in idle)
            {
                w.TrySetResult();
            }
        }
    }
}
