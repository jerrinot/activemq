package org.apache.activemq.leveldb.test;

import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.apache.activemq.leveldb.replicated.hazelcast.HazelcastMasterElector;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HazelcastMasterElectorTest extends ZooKeeperTestSupport {

    @Before
    public void resetDataDirs() throws IOException {
        deleteDirectory("node-1");
        deleteDirectory("node-2");
        deleteDirectory("node-3");
    }

    @Test
    public void testBasic() throws InterruptedException {
        ElectingLevelDBStore storeNode1 = createStoreNode("node-1");
        ElectingLevelDBStore storeNode2 = createStoreNode("node-2");
        ElectingLevelDBStore storeNode3 = createStoreNode("node-3");

        HazelcastMasterElector elector1 = new HazelcastMasterElector(storeNode1);
        HazelcastMasterElector elector2 = new HazelcastMasterElector(storeNode2);
        HazelcastMasterElector elector3 = new HazelcastMasterElector(storeNode3);

        elector1.startElector();
        elector2.startElector();
        elector3.startElector();

        Thread.sleep(10*1000);

        elector1.stopElector();

        Thread.sleep(10*1000);
    }

    protected void deleteDirectory(String s) throws IOException {
        try {
            FileUtils.deleteDirectory(new File(data_dir(), s));
        } catch (IOException e) {
        }
    }


    private ElectingLevelDBStore createStoreNode(String id) {
        ElectingLevelDBStore store = new ElectingLevelDBStore();
        store.setDirectory(new File(data_dir(), id));
        store.setContainer(id);
        store.setReplicas(3);
        store.setZkAddress("localhost:" + connector.getLocalPort());
        store.setHostname("localhost");
        store.setBind("tcp://0.0.0.0:0");
        return store;
    }
}
