package org.apache.activemq.store.leveldb;

import org.apache.activemq.leveldb.replicated.ElectingLevelDBStore;
import org.apache.activemq.leveldb.replicated.hazelcast.HazelcastElectingLevelDBStore;
import org.apache.activemq.store.PersistenceAdapter;


/**
 * An implementation of {@link org.apache.activemq.store.PersistenceAdapter} designed for use with
 * LevelDB - Embedded Lightweight Non-Relational Database.
 *
 * It does use Hazelcast to elect cluster master.
 *
 * @org.apache.xbean.XBean element="hazelcastReplicatedLevelDB"
 *
 */
public class HazelcastReplicatedLevelDBPersistenceAdapter extends HazelcastElectingLevelDBStore implements PersistenceAdapter {
}
