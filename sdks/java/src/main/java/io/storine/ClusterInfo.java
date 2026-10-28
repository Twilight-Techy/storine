package io.storine;

import java.util.Map;

/** The server currently used and its view of the cluster ({@code leaderId}/{@code leaderAddr} null when unknown). */
public record ClusterInfo(long nodeId, boolean isLeader, Long leaderId, String leaderAddr, long term, long commitIndex,
        long lastApplied, Map<Long, String> members) {}
