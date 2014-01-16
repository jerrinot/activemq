package org.apache.activemq.leveldb.replicated.hazelcast;

import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

final class StoreController {
    private static final Logger log = LoggerFactory.getLogger(StoreController.class);

    private final ElectingLevelDBStore store;
    private final NodeState nodeState;

    private int DEFAULT_OPERATIONAL_TIMEOUT = 5; //second

    public StoreController(ElectingLevelDBStore store, NodeState nodeState) {
        this.store = store;
        this.nodeState = nodeState;
    }

    synchronized void stopEverything() {
        if (nodeState.isMaster()) {
            stopMaster();
        } else if (nodeState.isSlave()) {
            stopSlave();
        }
    }

    synchronized void ensureSlave(String address) {
        String currentMasterAddress = nodeState.getCurrentMasterAddress();
        if (currentMasterAddress != null) {
            if (nodeState.isMaster()) {
                stopMaster();
            } else if (!currentMasterAddress.equals(address)) {
                stopSlave();
            }
        }
        if (nodeState.getCurrentMasterAddress() == null && address != null) {
            startSlave(address);
        }
    }

    synchronized void ensureMaster() {
        if (nodeState.isSlave()) {
            stopSlave();
        }
        if (!nodeState.isMaster()) {
            startMaster();
        }
    }


    private void stopMaster(final CountDownLatch latch) {
        log.info("Stopping master...");
        store.stop_master(new AbstractFunction0() {
            @Override
            public Object apply() {
                log.info("Master stopped...");
                nodeState.disconnected();
                latch.countDown();
                return BoxedUnit.UNIT;
            }
        });
    }

    private void stopSlave(final CountDownLatch latch) {
        log.info("Stopping slave.");
        store.stop_slave(new AbstractFunction0() {
            @Override
            public Object apply() {
                log.info("Slave has been stopped.");
                nodeState.disconnected();
                latch.countDown();
                return BoxedUnit.UNIT;
            }
        });
    }

    private void startSlave(final String address, final CountDownLatch latch) {
        log.info("Connecting as slave to '{}'", address);
        store.start_slave(address, new AbstractFunction0() {
            @Override
            public Object apply() {
                log.info("Connected to '{}'", address);
                nodeState.slave(address);
                latch.countDown();
                return BoxedUnit.UNIT;
            }
        });
    }

    private void startMaster(final CountDownLatch latch) {
        log.info("Starting master...");
        store.start_master(new AbstractFunction1<Object, BoxedUnit>() {
            @Override
            public BoxedUnit apply(Object port) {
                String address = store.address((Integer) port);
                log.info("Master started at '{}'", address);
                nodeState.master(address);
                latch.countDown();
                return BoxedUnit.UNIT;
            }
        });
    }

    private void startSlave(String address) {
        CountDownLatch latch = new CountDownLatch(1);
        startSlave(address, latch);
        awaitLatch(latch);
    }

    private void startMaster() {
        CountDownLatch latch = new CountDownLatch(1);
        startMaster(latch);
        awaitLatch(latch);
    }

    private void stopMaster() {
        CountDownLatch latch = new CountDownLatch(1);
        stopMaster(latch);
        awaitLatch(latch);
    }

    private void stopSlave() {
        CountDownLatch latch = new CountDownLatch(1);
        stopSlave(latch);
        awaitLatch(latch);
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(DEFAULT_OPERATIONAL_TIMEOUT, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout when awaiting latch...");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IllegalStateException(e);
        }
    }
}
