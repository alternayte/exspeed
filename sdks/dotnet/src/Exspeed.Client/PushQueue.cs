namespace Exspeed;

/// <summary>
/// The buffer behind a subscription: pushes go in on the connection's reader, the application takes them out
/// with <see cref="NextAsync"/>. Ending it wakes every waiter; with <c>keepBuffered</c>, what is already
/// buffered is still handed out first.
/// </summary>
internal sealed class PushQueue<T>
    where T : class
{
    private readonly object _gate = new();
    private readonly LinkedList<T> _items = new();
    private readonly LinkedList<TaskCompletionSource<T?>> _waiters = new();
    private readonly Action<T>? _onTaken;
    private bool _ended;
    private bool _dropped;

    public PushQueue(Action<T>? onTaken = null)
    {
        _onTaken = onTaken;
    }

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _items.Count;
            }
        }
    }

    /// <summary>Add items (ignored once ended); waiters get them first.</summary>
    public void Enqueue(IEnumerable<T> items)
    {
        List<(TaskCompletionSource<T?>, T)>? handoffs = null;
        lock (_gate)
        {
            if (_ended)
            {
                return;
            }
            foreach (var item in items)
            {
                if (_waiters.First is { } w)
                {
                    _waiters.RemoveFirst();
                    (handoffs ??= new()).Add((w.Value, item));
                }
                else
                {
                    _items.AddLast(item);
                }
            }
        }
        if (handoffs is not null)
        {
            foreach (var (tcs, item) in handoffs)
            {
                Handoff(tcs, item);
            }
        }
    }

    public void Enqueue(T item) => Enqueue(new[] { item });

    private void Handoff(TaskCompletionSource<T?> tcs, T item)
    {
        while (true)
        {
            if (tcs.TrySetResult(item))
            {
                _onTaken?.Invoke(item);
                return;
            }
            // The waiter gave up (timeout or cancellation) meanwhile: give the item to the next waiter, or
            // put it back at the front.
            lock (_gate)
            {
                if (_dropped)
                {
                    return;
                }
                if (_waiters.First is { } next)
                {
                    _waiters.RemoveFirst();
                    tcs = next.Value;
                    continue;
                }
                _items.AddFirst(item);
                return;
            }
        }
    }

    /// <summary>
    /// The next item, or <c>null</c> once ended (and drained) or when <paramref name="timeout"/> passes first.
    /// </summary>
    public async Task<T?> NextAsync(TimeSpan? timeout, CancellationToken cancellationToken)
    {
        TaskCompletionSource<T?> tcs;
        LinkedListNode<TaskCompletionSource<T?>> node;
        T? ready = null;
        lock (_gate)
        {
            if (_items.First is { } first)
            {
                _items.RemoveFirst();
                ready = first.Value;
            }
            else if (_ended)
            {
                return null;
            }
            tcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
            node = ready is null ? _waiters.AddLast(tcs) : new LinkedListNode<TaskCompletionSource<T?>>(tcs);
        }
        if (ready is not null)
        {
            _onTaken?.Invoke(ready);
            return ready;
        }
        try
        {
            if (timeout is { } t)
            {
                return await tcs.Task.WaitAsync(t, cancellationToken).ConfigureAwait(false);
            }
            return await tcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is TimeoutException || e is OperationCanceledException)
        {
            bool removedHere = false;
            lock (_gate)
            {
                if (node.List is not null)
                {
                    _waiters.Remove(node);
                    removedHere = true;
                }
            }
            if (!removedHere && !tcs.TrySetResult(null) && tcs.Task.Result is { } got)
            {
                // An item was handed over just as we gave up: take it.
                return got;
            }
            if (e is TimeoutException)
            {
                return null;
            }
            throw;
        }
    }

    /// <summary>End the queue; waiters wake with <c>null</c> once the kept items are taken.</summary>
    public void End(bool keepBuffered)
    {
        List<TaskCompletionSource<T?>>? wake = null;
        lock (_gate)
        {
            if (_ended)
            {
                return;
            }
            _ended = true;
            if (!keepBuffered)
            {
                _dropped = true;
                _items.Clear();
            }
            if (_items.Count == 0)
            {
                wake = _waiters.ToList();
                _waiters.Clear();
            }
        }
        if (wake is not null)
        {
            foreach (var w in wake)
            {
                w.TrySetResult(null);
            }
        }
    }

    /// <summary>Drop every buffered item.</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _items.Clear();
        }
    }
}
