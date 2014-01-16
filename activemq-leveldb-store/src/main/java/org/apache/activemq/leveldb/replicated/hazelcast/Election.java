package org.apache.activemq.leveldb.replicated.hazelcast;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

public class Election {
    private String electionId = "";
    private final int quorum;

    private SortedSet<NodePosition> nodePositions = new TreeSet<NodePosition>();

    public Election(int quorum) {
        this.quorum = quorum;
    }

    public String newElection(String uuid, long position) {
        synchronized (electionId) {
            electionId = UUID.randomUUID().toString();
            nodePositions = new TreeSet<NodePosition>();
            nodePositions.add(new NodePosition(uuid, position));
            return electionId;
        }
    }

    public String tryElect(String receivedElectionId, String nodeUuid, long position) {
        String electedNode = null;
        synchronized (receivedElectionId) {
            if (isValidElectionId(receivedElectionId)) {
                nodePositions.add(new NodePosition(nodeUuid, position));
                if (nodePositions.size() >= quorum) {
                    electedNode = nodePositions.last().getUuid();
                    electionId = "";
                }
            }
        }
        return electedNode;
    }

    private boolean isValidElectionId(String id) {
        return electionId.equals(id);
    }

}
