package org.apache.activemq.leveldb.replicated.hazelcast;

import com.hazelcast.core.HazelcastInstance;

interface Protocol {
    void start(HazelcastInstance hazelcastInstance, MessageProcessor messageProcessor);

    void stop();

    void requestMasterAddress();

    void publishMasterAddress(String s);

    void publishState(String electionId, long position);

    void publishElected(String electedNode);

    void publishPositionRequest(String electionId);

    void publishNewMemberConnected();
}
