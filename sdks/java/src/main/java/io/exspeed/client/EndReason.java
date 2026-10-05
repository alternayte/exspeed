package io.exspeed.client;

/**
 * Why a subscription ended.
 *
 * <p>{@code 404}: the consumer or its stream was deleted. {@code 503}: the node
 * lost leadership, or the connection was lost and not re-established.
 * {@code 0}: ended locally ({@code unsubscribe()}, {@code close()} or the client
 * closing). Other codes come from a failed re-subscribe after a reconnect.
 *
 * @param code the code
 * @param message a description
 */
public record EndReason(int code, String message) {}
