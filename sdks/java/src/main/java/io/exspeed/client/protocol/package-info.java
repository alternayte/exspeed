/**
 * The binary encoding of client protocol v2 (frames, requests, responses and
 * records), mirroring {@code crates/exspeed-protocol/src/client.rs}.
 *
 * <p>This package is the low-level layer under {@link io.exspeed.client.ExspeedClient}.
 * It encodes and decodes both directions, so it also serves tools and test
 * servers. Applications normally don't need it.
 */
package io.exspeed.client.protocol;
