package org.apache.activemq.leveldb.replicated.hazelcast;

import com.hazelcast.core.Member;

public interface MessageProcessor {
    void processMasterAddressReply(Member src, String message);
    void processMasterAddressRequest(Member src, String message);
    void processPositionRequest(Member src, String message);
    void processPositionReply(Member src, String electionId, long position);
    void processMasterElected(Member src, String message);
    void processNewMemberConnected(Member src, String message);
}
