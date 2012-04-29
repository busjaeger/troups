package edu.illinois.troups.tm.impl;

import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_DELETE;
import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_GET;
import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_PUT;
import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_STATE_TRANSITION;
import static edu.illinois.troups.tm.TransactionState.ABORTED;
import static edu.illinois.troups.tm.TransactionState.COMMITTED;
import static edu.illinois.troups.tm.TransactionState.FINALIZED;
import static edu.illinois.troups.tm.TransactionState.STARTED;
import static edu.illinois.troups.tm.impl.TransientTransactionState.BLOCKED;
import static edu.illinois.troups.tm.impl.TransientTransactionState.CREATED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.TransactionLog;
import edu.illinois.troups.tm.TransactionLog.GetRecord;
import edu.illinois.troups.tm.TransactionLog.OperationRecord;
import edu.illinois.troups.tm.TransactionLog.Record;
import edu.illinois.troups.tm.TransactionLog.StateTransitionRecord;
import edu.illinois.troups.tm.TransactionManager;
import edu.illinois.troups.tm.TransactionOperationObserver;
import edu.illinois.troups.tsm.TimestampManager;
import edu.illinois.troups.tsm.TimestampManager.TimestampReclamationListener;

/**
 * Note: this class (and all other classes in this package) do not depend on
 * HBase. This ensures separation of protocol from implementation, so let's keep
 * it that way, unless there is a good reason not to!
 * 
 * A generic transaction manager that implements the Multiversion Timestamp
 * Ordering Protocol (MVTO) as described in 'Transactional Information Systems'
 * by Weikum and Vossen in Section 5.5. The protocol consists of the following
 * three rules:
 * 
 * <ol>
 * <li>A read by transaction i on object x [= r_i(x)] is transformed into a read
 * by transaction i on the version k of x [= r_i(x_k)], where k is the version
 * of x that carries the largest timestamp less than or equal to the timestamp
 * of transaction i [= ts(x_k) <= ts(t_i)] and was written by transaction k, k
 * != i.</li>
 * <li>A write by transaction i [= w_i(x)] is processed as follows:
 * <ol>
 * <li>If a transaction j has read a version k of x [= r_j(x_k)] such that the
 * timestamp of transaction k is smaller than that of i and the timestamp of i
 * smaller than that of j [= ts(t_k) < ts(t_i) < ts(t_j), then the write is
 * rejected and transaction i is aborted.</li>
 * <li>Otherwise the write of x is transformed into a write of version i of x [=
 * w_i(x_i)].</li>
 * </ol>
 * <li>A commit of transaction i is delayed until the commit of all transactions
 * that have written new versions of data items read by transaction i.</li>
 * </ol>
 * 
 * Schedules produced by this protocol are view serialiable. One implication of
 * this is that blind reads are not necessarily serialized. If blind reads are
 * not allowed, then it produces conflict serializable schedules.
 * 
 * If we wanted to support blind writes, we may be able to add an implicit read
 * for every write that does not have a matching read preceding it in its
 * transaction.
 * 
 * Current assumption made by the implementation (unchecked):
 * <ol>
 * <li>transactions execute a read, write, or delete only once per Key
 * <li>transactions execute either a write or a delete for a Key, not both
 * <li>transactions always execute a read for a Key before writing/deleting it
 * </ol>
 * 
 * 
 * TODO (in order of priority):
 * <ol>
 * <li>remove implementation assumptions (see above)
 * 
 * <li>support alternate policy to only read committed versions (to eliminate
 * cascading aborts)
 * 
 * <li>think about consequences of IOExceptions
 * <li>think about if there is a way to do finalize asynchronously
 * <li>think about reading (1) conflicting write in progress and (2) delete from
 * removed finalized transactions
 * <li>refactor to clearly separate: local transaction processing, distributed
 * transactions processing, and concurrency control policy
 * <li>follow up with HBase dev team to get get/put failed notifiers
 * 
 * </ol>
 */
public class MVTOTransactionManager<K extends Key, R extends Record<K>>
    implements TransactionManager, TransactionOperationObserver<K>,
    TimestampReclamationListener {

  private static final Log LOG = LogFactory
      .getLog(MVTOTransactionManager.class);

  // immutable state
  // key value store this TM is governing
  protected final KeyValueStore<K> keyValueStore;
  // transaction log
  protected final TransactionLog<K, R> transactionLog;
  // timestamp manager
  protected final TimestampManager timestampManager;
  // used to order transactions by timestamp
  protected final Comparator<MVTOTransaction<K>> timestampComparator;

  // mutable state
  // transactions by transaction ID for efficient direct lookup
  protected final NavigableMap<TID, MVTOTransaction<K>> transactions;
  // TAs indexed by key and versions read for efficient conflict detection
  protected final Map<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> readers;
  // TAs indexed by key currently being written for efficient conflict detection
  protected final Map<K, NavigableSet<MVTOTransaction<K>>> activeWriters;
  // sequence of reading and finalized transactions to synchronize TA removal
  // TODO implement more efficiently (delay work to GC time)
  protected final Queue<MVTOTransaction<K>> activeReaders;
  // queue of transactions ready to be reclaimed
  protected final Set<MVTOTransaction<K>> reclaimables;
  // lock protect the previous two conflict detection data structures
  protected final ConcurrentMap<Key, Lock> keyLocks;
  // flag to indicate whether this TM is running
  protected boolean running;
  // guards the running flag
  protected ReadWriteLock runLock = new ReentrantReadWriteLock();

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore,
      TransactionLog<K, R> transactionLog, TimestampManager tsm) {
    this.keyValueStore = keyValueStore;
    this.transactionLog = transactionLog;
    this.timestampManager = tsm;
    this.timestampComparator = new Comparator<MVTOTransaction<K>>() {
      @Override
      public int compare(MVTOTransaction<K> t1, MVTOTransaction<K> t2) {
        return timestampManager.compare(t1.getID().getTS(), t2.getID().getTS());
      }
    };

    this.transactions = new TreeMap<TID, MVTOTransaction<K>>();
    this.readers = new HashMap<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
    this.activeWriters = new HashMap<K, NavigableSet<MVTOTransaction<K>>>();
    this.activeReaders = new ConcurrentLinkedQueue<MVTOTransaction<K>>();
    this.reclaimables = new HashSet<MVTOTransaction<K>>();
    this.keyLocks = new ConcurrentHashMap<Key, Lock>();
  }

  public KeyValueStore<K> getKeyValueStore() {
    return keyValueStore;
  }

  public TransactionLog<K, R> getTransactionLog() {
    return transactionLog;
  }

  public TimestampManager getTimestampManager() {
    return timestampManager;
  }

  public void start() {
    runLock.writeLock().lock();
    try {
      if (running)
        return;
      NavigableMap<Long, R> records = transactionLog.open();
      for (Entry<Long, R> record : records.entrySet()) {
        long sid = record.getKey();
        R r = record.getValue();
        replay(sid, r);
      }
      for (MVTOTransaction<K> ta : transactions.values())
        recover(ta);
      this.timestampManager.addTimestampReclamationListener(this);
      running = true;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } finally {
      runLock.writeLock().unlock();
    }
  }

  private long tTime;
  private long tNum;

  void timestampTime(long time) {
    tNum++;
    tTime += time;
  }

  // TODO
  public void stop() {
    if (!runLock.writeLock().tryLock()) {
      while (true) {
        for (MVTOTransaction<K> ta : transactions.values())
          ta.unblock();
        try {
          if (runLock.writeLock().tryLock(100, TimeUnit.MILLISECONDS))
            break;
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
    }
    try {
      if (!running)
        return;
      running = false;
      LOG.info("Timestamp time " + (tNum > 0 ? tTime / tNum : 0));
    } finally {
      runLock.writeLock().unlock();
    }
  }

  // TODO
  public void abort() {
    // we could probably do without this
    for (MVTOTransaction<K> ta : transactions.values())
      try {
        ta.releaseTimestamp();
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  public void stopped() {
    // nothing to do here
  }

  @Override
  public void beforeGet(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforeGet(keys);
      }
    }.run(tid);
  }

  @Override
  public void afterGet(TID tid, final Iterable<? extends KeyVersions<K>> kvs)
      throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterGet(kvs);
      }
    }.run(tid);
  }

  @Override
  public void failedGet(TID tid, final Iterable<? extends K> keys,
      final Throwable t) throws TransactionAbortedException, IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.failedGet(keys, t);
      }
    }.run(tid);
  }

  @Override
  public void beforePut(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforePut(keys);
      }
    }.run(tid);
  }

  @Override
  public void afterPut(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterPut(keys);
      }
    }.run(tid);
  }

  @Override
  public void failedPut(TID tid, final Iterable<? extends K> keys,
      final Throwable t) throws TransactionAbortedException, IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.failedPut(keys, t);
      }
    }.run(tid);
  }

  @Override
  public void beforeDelete(TID tid, final Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforeDelete(keys);
      }
    }.run(tid);
  }

  @Override
  public void failedDelete(TID tid, final Iterable<? extends K> keys,
      final Throwable t) throws TransactionAbortedException, IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.failedDelete(keys, t);
      }
    }.run(tid);
  }

  @Override
  public void afterDelete(TID tid, final Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterDelete(keys);
      }
    }.run(tid);
  }

  @Override
  public TID begin() throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      MVTOTransaction<K> ta = new MVTOTransaction<K>(this);
      ta.begin();
      addTransaction(ta);
      return ta.getID();
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void commit(final TID tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.commit();
        System.out.println("Committed " + tid);
      }
    }.run(tid);
  }

  @Override
  public void abort(final TID tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.abort();
        System.out.println("Aborted " + tid);
      }
    }.run(tid);
  }

  abstract class WithReadLock {
    void run(TID id) throws IOException {
      runLock.readLock().lock();
      try {
        checkRunning();
        MVTOTransaction<K> ta = getTransaction(id);
        if (ta == null)
          throw new IllegalStateException("Transaction " + id
              + " does not exist");
        execute(ta);
      } finally {
        runLock.readLock().unlock();
      }
    }

    void execute(MVTOTransaction<K> ta) throws IOException {
      // overwrite
    }
  }

  void checkRunning() {
    runLock.readLock().lock();
    try {
      if (!running)
        throw new IllegalStateException("Transaction Manager stopped");
    } finally {
      runLock.readLock().unlock();
    }
  }

  Lock getLock() {
    return runLock.readLock();
  }

  void lock(Key key) {
    Lock keyLock = keyLocks.get(key);
    if (keyLock == null) {
      keyLock = new ReentrantLock();
      Lock raceLock = keyLocks.putIfAbsent(key, keyLock);
      if (raceLock != null)
        keyLock = raceLock;
    }
    keyLock.lock();
  }

  void unlock(Key key) {
    Lock keyLock = keyLocks.remove(key);
    if (keyLock != null)
      keyLock.unlock();
  }

  void addTransaction(MVTOTransaction<K> ta) {
    synchronized (transactions) {
      transactions.put(ta.getID(), ta);
    }
  }

  /**
   * Get the transaction object for the given timestamp
   * 
   * @param tid
   * @return
   */
  MVTOTransaction<K> getTransaction(TID tid) {
    synchronized (transactions) {
      return transactions.get(tid);
    }
  }

  Iterable<MVTOTransaction<K>> getTransactions() {
    List<MVTOTransaction<K>> snapshot;
    synchronized (transactions) {
      snapshot = new ArrayList<MVTOTransaction<K>>(transactions.values());
    }
    return snapshot;
  }

  MVTOTransaction<K> removeTransaction(MVTOTransaction<K> ta) {
    synchronized (transactions) {
      return transactions.remove(ta.getID());
    }
  }

  // must be called with key lock held
  void addReader(K key, long version, MVTOTransaction<K> reader) {
    NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions;
    synchronized (readers) {
      versions = readers.get(key);
      if (versions == null)
        readers.put(key,
            versions = new TreeMap<Long, NavigableSet<MVTOTransaction<K>>>());
    }
    NavigableSet<MVTOTransaction<K>> readers = versions.get(version);
    if (readers == null)
      versions.put(version, readers = new TreeSet<MVTOTransaction<K>>(
          timestampComparator));
    readers.add(reader);
  }

  // must be called with key lock held
  NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> getReaders(K key) {
    synchronized (readers) {
      return readers.get(key);
    }
  }

  // must be called with key lock held
  void removeReader(Key key, long version, MVTOTransaction<K> reader) {
    synchronized (readers) {
      NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = readers
          .get(key);
      if (versions != null) {
        NavigableSet<MVTOTransaction<K>> tas = versions.get(version);
        if (tas != null) {
          tas.remove(reader);
          if (tas.isEmpty()) {
            versions.remove(version);
            if (versions.isEmpty())
              readers.remove(key);
          }
        }
      }
    }
  }

  // must hold key lock to call this method
  void addActiveWriter(K key, MVTOTransaction<K> writer) {
    NavigableSet<MVTOTransaction<K>> writes;
    synchronized (activeWriters) {
      writes = activeWriters.get(key);
      if (writes == null)
        activeWriters.put(key, writes = new TreeSet<MVTOTransaction<K>>(
            timestampComparator));
    }
    writes.add(writer);
  }

  // must hold key lock to call this method
  NavigableSet<MVTOTransaction<K>> getActiveWriters(K key) {
    synchronized (activeWriters) {
      return activeWriters.get(key);
    }
  }

  // must hold key lock to call this method
  void removeActiveWriter(Key key, MVTOTransaction<K> writer) {
    synchronized (activeWriters) {
      NavigableSet<MVTOTransaction<K>> writes = activeWriters.get(key);
      if (writes != null) {
        writes.remove(writer);
        if (writes.isEmpty())
          activeWriters.remove(key);
      }
    }
  }

  void addActiveReader(MVTOTransaction<K> ta) {
    activeReaders.add(ta);
  }

  void removeActiveReader(MVTOTransaction<K> ta) {
    activeReaders.remove(ta);
  }

  // garbage collection
  @Override
  public void reclaimed(long ts) {
    updateReclaimables();
    Long cutoff = null;
    for (MVTOTransaction<K> ta : getTransactions()) {
      long sid = ta.getSID();
      if (timestampManager.compare(ta.getID().getTS(), ts) <= 0) {
        // find the smallest sequence number of transactions that are reclaimed
        if (cutoff == null || transactionLog.compare(cutoff, sid) < 0)
          cutoff = sid;
        reclaim(ta);
      } else {
        if (cutoff == null)
          break;
        // if any transaction with higher timestamp (started later globally)
        // has a lower sequence number (started earlier locally), increase the
        // cutoff, because we don't want to discard its log records
        if (transactionLog.compare(cutoff, sid) > 0)
          cutoff = ta.getSID();
      }
    }
    if (cutoff != null)
      try {
        transactionLog.truncate(cutoff);
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  private void updateReclaimables() {
    Iterator<MVTOTransaction<K>> it = activeReaders.iterator();
    while (it.hasNext()) {
      MVTOTransaction<K> ta = it.next();
      if (ta.getState() != FINALIZED)
        break;
      reclaimables.add(ta);
      it.remove();
    }
  }

  protected void reclaim(MVTOTransaction<K> ta) {
    switch (ta.getState()) {
    case CREATED:
      removeTransaction(ta);
      break;
    case STARTED:
    case BLOCKED:
      System.out.println("WARNING: found active TAs before oldest timestamp "
          + ta);
      try {
        ta.abort();
      } catch (IOException e) {
        e.printStackTrace();
      }
      break;
    case ABORTED:
    case COMMITTED:
      // still needs to be finalized
      break;
    case FINALIZED:
      /*
       * reads can only be cleaned up once all transactions that started before
       * this one have completed
       */
      ta.removeReads();
      if (reclaimables.remove(ta))
        removeTransaction(ta);
      break;
    }
  }

  protected void replay(long sid, R record) {
    TID tid = record.getTID();
    int type = record.getType();
    MVTOTransaction<K> ta = transactions.get(tid);
    switch (type) {
    case RECORD_TYPE_STATE_TRANSITION: {
      StateTransitionRecord<K> str = (StateTransitionRecord<K>) record;
      switch (str.getTransactionState()) {
      case STARTED:
        if (ta != null)
          throw new IllegalStateException(
              "begin record for existing transaction");
        ta = new MVTOTransaction<K>(this);
        ta.setStarted(tid, sid);
        transactions.put(ta.getID(), ta);
        return;
      case COMMITTED:
        if (ta == null)
          return;
        ta.setCommitted();
        return;
      case ABORTED:
        if (ta == null)
          return;
        ta.setAborted();
        return;
      case FINALIZED:
        if (ta == null)
          return;
        switch (ta.getState()) {
        case COMMITTED:
          ta.setCommitFinalized();
          break;
        case ABORTED:
          ta.setAbortFinalized();
          break;
        }
        return;
      }
      return;
    }
    case RECORD_TYPE_GET: {
      if (ta == null)
        return;
      GetRecord<K> glr = (GetRecord<K>) record;
      K key = glr.getKey();
      long version = glr.getVersion();
      ta.addGet(key, version);
      return;
    }
    case RECORD_TYPE_PUT:
    case RECORD_TYPE_DELETE: {
      if (ta == null)
        return;
      OperationRecord<K> olr = (OperationRecord<K>) record;
      K key = olr.getKey();
      boolean isDelete = olr.getType() == RECORD_TYPE_DELETE;
      ta.addMutation(key, isDelete);
      return;
    }
    }
    throw new IllegalStateException("Invalid log record: " + record);
  }

  // technically we don't need to abort
  protected void recover(MVTOTransaction<K> ta) {
    try {
      TID tid = ta.getID();
      switch (ta.getState()) {
      case CREATED:
        throw new IllegalStateException("Created transaction during recovery");
      case STARTED:
        // TODO check if any operations failed in the middle
        if (!timestampManager.isHeldByCaller(tid.getTS()))
          ta.abort();
      case BLOCKED:
        throw new IllegalStateException("Blocked transaction during recovery");
      case ABORTED:
      case COMMITTED:
        ta.finalize();
        break;
      case FINALIZED:
        // nothing to do here, but be happy
        break;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}