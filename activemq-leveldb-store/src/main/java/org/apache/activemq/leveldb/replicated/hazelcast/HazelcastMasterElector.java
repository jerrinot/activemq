package org.apache.activemq.leveldb.replicated.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HazelcastMasterElector implements MessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(HazelcastMasterElector.class);

    private final ElectingLevelDBStore store;

    private HazelcastInstance hazelcastInstance;
    private Member localMember;

    private volatile boolean masterElected = false;

    private final NodeState nodeState;
    private final Election election;
    private final StoreController controller;
    private final Protocol protocol;

    public HazelcastMasterElector(ElectingLevelDBStore store) {
        this.store = store;
        this.protocol = new SimpleTopicProtocol();
        this.nodeState = new NodeState();
        this.controller = new StoreController(store, nodeState);
        this.election = new Election(store.clusterSizeQuorum());
    }

    public void startElector() {
        log.info("Creating new elector");
        startHazelcastInstance();
        protocol.start(hazelcastInstance, this);
        localMember = hazelcastInstance.getCluster().getLocalMember();

        hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddedEvent(membershipEvent);
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedEvent(membershipEvent);
            }
        });
        nodeState.started();
        protocol.publishNewMemberConnected();

    }

    private void startHazelcastInstance() {
        Config hazelcastConfig = new Config();
        String brokerName = store.brokerName();
        if (brokerName != null && !brokerName.isEmpty()) {
            hazelcastConfig.setGroupConfig(new GroupConfig(brokerName, brokerName));
        }
        hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig);
    }

    private void memberAddedEvent(MembershipEvent membershipEvent) {
        int size = membershipEvent.getMembers().size();
        int replicas = store.replicas();
        if (size > replicas) {
            log.warn("Only {} replicas configured, but {} nodes joined the cluster!", replicas, size);
        }
    }

    private void memberRemovedEvent(MembershipEvent membershipEvent) {
        if (nodeState.isStopped()) {
            return;
        }
        masterElected = false;
        if (isElector(membershipEvent.getMembers())) { //TODO: If the removed member wasn't master or elector we don't need to do anything - as long as there is still a quorum
            controller.stopEverything();
            if (!isLocalNode(membershipEvent.getMember().getUuid())) {
                startNewElection();
            }
        }
    }

    @Override
    public void processMasterAddressReply(Member src, String address) {
        if (nodeState.isStopped()) {
            return;
        }
        controller.ensureSlave(address);
    }

    @Override
    public void processMasterAddressRequest(Member src, String address) {
        if (nodeState.isStopped()) {
            return;
        }
        String masterAddress = nodeState.getCurrentMasterAddress();
        if (nodeState.isMaster() && masterAddress != null) {
            protocol.publishMasterAddress(masterAddress);
        }
    }

    @Override
    public void processPositionRequest(Member src, String electionId) {
        if (nodeState.isStopped()) {
            return;
        }
        controller.stopEverything();
        protocol.publishState(electionId, store.position());
    }

    @Override
    public void processMasterElected(Member src, String electedUUID) {
        if (nodeState.isStopped()) {
            return;
        }
        if (isLocalNode(electedUUID)) {
            ensureMasterAndPusblish();
        }
    }

    @Override
    public void processNewMemberConnected(Member src, String message) {
        if (nodeState.isStopped()) {
            return;
        }
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        if (isQuorum(members) && isElector(members)) {
            if (masterElected) {
                protocol.requestMasterAddress();
            } else {
                startNewElection();
            }
        }
    }

    @Override
    public void processPositionReply(Member src, String receivedElectionId, long position) {
        if (nodeState.isStopped()) {
            return;
        }
        if (isElector()) {
            String electedNode = election.tryElect(receivedElectionId, src.getUuid(), position);
            if (electedNode != null) {
                masterElected = true;
                if (isLocalNode(electedNode)) {
                    ensureMasterAndPusblish();
                } else {
                    protocol.publishElected(electedNode);
                }
            }
        }
    }

    public void stopElector() {
        log.info("Stopping elector.");
        if (nodeState.stop()) {
            hazelcastInstance.shutdown();
            controller.stopEverything();
        }
    }

    private void ensureMasterAndPusblish() {
        controller.ensureMaster();
        protocol.publishMasterAddress(nodeState.getCurrentMasterAddress());
    }

    private void startNewElection() {
        String electionId = election.newElection(localMember.getUuid(), store.position());
        protocol.publishPositionRequest(electionId);
    }

    private boolean isElector() {
        return isElector(hazelcastInstance.getCluster().getMembers());
    }

    private boolean isElector(Set<Member> members) {
        Iterator<Member> iterator = members.iterator();
        return iterator.hasNext() && iterator.next().equals(localMember); //the oldest member is always first
    }

    private boolean isQuorum(Set<Member> members) {
        return members.size() >= store.clusterSizeQuorum();
    }

    private boolean isLocalNode(String uuid) {
        log.info("Checking is '{}' is same as localnode '{}'", uuid, localMember.getUuid());
        return uuid.equals(localMember.getUuid());
    }

}
