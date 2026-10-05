package io.exspeed.client;

/**
 * From the server's handshake reply.
 *
 * @param serverVersion the server's version
 * @param nodeId the node's id
 * @param leader the leader's client address when the connected node is not the leader, or {@code null}
 */
public record ServerInfo(String serverVersion, String nodeId, String leader) {}
