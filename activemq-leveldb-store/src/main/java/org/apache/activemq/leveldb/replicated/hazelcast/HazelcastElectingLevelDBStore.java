package org.apache.activemq.leveldb.replicated.hazelcast;

import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;

public class HazelcastElectingLevelDBStore extends ElectingLevelDBStore {

    private HazelcastMasterElector elector;

    @Override
    public void registerJMX() {
        //TODO: JMX for Hazelcast Store
    }

    @Override
    public void initElector() {
        elector = new HazelcastMasterElector(this);
        elector.startElector();
    }

    @Override
    public void stopElector() {
        elector.stopElector();
    }
}
