package io.exspeed.client;

/**
 * Node id, leadership and server version, from {@link ExspeedClient#metadata()}.
 *
 * @param nodeId the node's id
 * @param isLeader whether this node is the leader
 * @param leader the leader's client address, when known and not this node
 * @param serverVersion the server's version
 */
public record Metadata(String nodeId, boolean isLeader, String leader, String serverVersion) {}
