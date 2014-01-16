package org.apache.activemq.leveldb.replicated.hazelcast;

import java.io.Serializable;

public class NodePosition implements Comparable<NodePosition>, Serializable{
    private String uuid;
    private long position;

    public NodePosition(String uuid, long position) {
        this.uuid = uuid;
        this.position = position;
    }

    public String getUuid() {
        return uuid;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "NodePosition{" +
                "uuid='" + uuid + '\'' +
                ", position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodePosition nodePosition = (NodePosition) o;
        if (!uuid.equals(nodePosition.uuid)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public int compareTo(NodePosition other) {
        int positionCompare = Long.compare(this.position, other.position);
        return positionCompare == 0 ? this.uuid.compareTo(other.uuid) : positionCompare;
    }
}