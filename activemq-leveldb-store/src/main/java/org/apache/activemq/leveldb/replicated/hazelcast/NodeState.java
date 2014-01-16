package org.apache.activemq.leveldb.replicated.hazelcast;

import java.util.concurrent.atomic.AtomicBoolean;

public class NodeState {

    private enum NodeMode {
        MASTER, SLAVE, DISCONNECTED
    }

    private NodeMode nodeMode;
    private String currentMasterAddress;
    private final AtomicBoolean stoppedOrStopping = new AtomicBoolean(true);

    public boolean isMaster() {
        return NodeMode.MASTER.equals(nodeMode);
    }

    public boolean isSlave() {
        return NodeMode.SLAVE.equals(nodeMode);
    }

    public String getCurrentMasterAddress() {
        return currentMasterAddress;
    }

    public void master(String currentMasterAddress) {
        this.nodeMode = NodeMode.DISCONNECTED;
        this.currentMasterAddress = currentMasterAddress;
    }

    public void slave(String currentMasterAddress) {
        this.nodeMode = NodeMode.SLAVE;
        this.currentMasterAddress = currentMasterAddress;
    }

    public void disconnected() {
        this.nodeMode = NodeMode.DISCONNECTED;
        this.currentMasterAddress = null;
    }

    public void started() {
        stoppedOrStopping.set(false);
    }

    public boolean stop() {
        return stoppedOrStopping.compareAndSet(false, true);
    }

    public boolean isStopped() {
        return stoppedOrStopping.get();
    }

}
