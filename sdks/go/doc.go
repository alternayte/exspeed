// Package exspeed is the Go client for Exspeed, a message broker with an
// integrated stream-processing engine. It speaks the binary client
// protocol v2 over TCP or TLS (docs/protocol.md in the Exspeed repository).
//
// One [Client] is one connection. Requests are multiplexed, so a long pull
// or long-poll read never blocks other calls; share one client across your
// application. Every blocking call takes a context.Context.
//
// Features:
//   - streams: create (with limits, TTLs and retention policies), update,
//     info, list, delete;
//   - publishing: single records, batches, idempotent msg ids, TTL, delay,
//     deliver-at and priority options, and a coalescing [Publisher];
//   - stateless reads with subject filters and long-polling;
//   - durable consumers with push ([Subscription], credit-windowed) and pull
//     delivery, acks, nacks, terms, redelivery and dead-lettering;
//   - bounded ExQL queries;
//   - core (non-persistent) publish/subscribe, queue groups and
//     request-reply;
//   - key-value buckets ([KV]) with revisions, compare-and-set, history and
//     watches;
//   - automatic reconnection that restores subscriptions.
//
// Quick start:
//
//	client, err := exspeed.Connect(ctx, "127.0.0.1:5933")
//	if err != nil { ... }
//	defer client.Close()
//
//	err = client.CreateStream(ctx, exspeed.StreamSpec{Name: "orders"})
//	res, err := client.Publish(ctx, "orders", exspeed.PublishRecord{
//		Subject: "orders.placed",
//		Value:   []byte(`{"id":42}`),
//	})
//
//	_, err = client.CreateConsumer(ctx, exspeed.ConsumerSpec{Name: "billing", Stream: "orders"})
//	sub, err := client.Subscribe(ctx, "billing", exspeed.SubscribeOptions{})
//	for {
//		msg, err := sub.Next(ctx)
//		if err != nil { break }
//		handle(msg)
//		msg.Ack()
//	}
//
// Errors are values: a rejected request is a [*ServerError] carrying the
// server's code (compare with errors.Is against [ErrNotFound],
// [ErrConflict], ...), and the client returns [*TimeoutError] and
// [*ConnectionError] for timeouts and connection problems.
package exspeed
