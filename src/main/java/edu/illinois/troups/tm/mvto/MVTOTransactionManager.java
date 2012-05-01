package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.TransactionState.ABORTED;
import static edu.illinois.troups.tm.TransactionState.COMMITTED;
import static edu.illinois.troups.tm.TransactionState.FINALIZED;
import static edu.illinois.troups.tm.TransactionState.STARTED;
import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_GET;
import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_PUT;
import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_STATE_TRANSITION;
import static edu.illinois.troups.tm.mvto.TransientTransactionState.BLOCKED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.TransactionManager;
import edu.illinois.troups.tm.TransactionOperationObserver;
import edu.illinois.troups.tm.log.TransactionLog;
import edu.illinois.troups.tm.log.TransactionLog.GetRecord;
import edu.illinois.troups.tm.log.TransactionLog.OperationRecord;
import edu.illinois.troups.tm.log.TransactionLog.Record;
import edu.illinois.troups.tm.log.TransactionLog.StateTransitionRecord;
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
  // used to compare transaction IDs
  protected final Comparator<TID> tidComparator;
  // used to order transactions by timestamp
  protected final Comparator<MVTOTransaction<K>> transactionComparator;
  // pool for scheduling finalization
  protected final ExecutorService pool;

  // mutable state
  // transactions by transaction ID for efficient direct lookup
  protected final ConcurrentNavigableMap<TID, MVTOTransaction<K>> transactions;
  // TAs indexed by key and versions read for efficient conflict detection
  protected final Map<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> readers;
  // TAs indexed by key currently being written for efficient conflict detection
  protected final Map<K, NavigableSet<MVTOTransaction<K>>> activeWriters;
  // lock protect the previous two conflict detection data structures
  protected final ConcurrentMap<Key, ReentrantReadWriteLock> keyLocks;
  // flag to indicate whether TA is closing
  protected boolean closing;
  // flag to indicate whether this TM is running
  protected boolean running;
  // guards the running flag
  protected ReadWriteLock runLock = new ReentrantReadWriteLock();
  // last locally reclaimed transaction
  protected long reclaimed;
  //
  protected ReadWriteLock reclaimLock = new ReentrantReadWriteLock();

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore,
      TransactionLog<K, R> transactionLog, TimestampManager tsm,
      ExecutorService pool) {
    this.keyValueStore = keyValueStore;
    this.transactionLog = transactionLog;
    this.timestampManager = tsm;
    this.pool = pool;
    this.tidComparator = TID.newComparator(tsm);
    this.transactionComparator = new Comparator<MVTOTransaction<K>>() {
      @Override
      public int compare(MVTOTransaction<K> t1, MVTOTransaction<K> t2) {
        return tidComparator.compare(t1.getID(), t2.getID());
      }
    };

    this.transactions = new ConcurrentSkipListMap<TID, MVTOTransaction<K>>(
        tidComparator);
    this.readers = new HashMap<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
    this.activeWriters = new HashMap<K, NavigableSet<MVTOTransaction<K>>>();
    this.keyLocks = new ConcurrentHashMap<Key, ReentrantReadWriteLock>();
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

  public ExecutorService getPool() {
    return pool;
  }

  public void start() {
    runLock.readLock().lock();
    try {
      if (running)
        return;
    } finally {
      runLock.readLock().unlock();
    }
    runLock.writeLock().lock();
    try {
      if (running)
        return;
      // first replay log records
      NavigableMap<Long, R> records = transactionLog.open();
      for (Entry<Long, R> record : records.entrySet()) {
        long sid = record.getKey();
        R r = record.getValue();
        replay(sid, r);
      }

      // next recover any transactions
      for (MVTOTransaction<K> ta : transactions.values())
        recover(ta);

      // finally run round of reclamation to remove obsolete transactions
      long ts = timestampManager.getLastReclaimedTimestamp();
      if (transactions.isEmpty())
        this.reclaimed = ts;
      else
        reclaimed(ts);

      running = true;
      closing = false;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start transaction manager", e);
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

  public void stopping() {
    runLock.readLock().lock();
    try {
      if (closing || !running)
        return;
    } finally {
      runLock.readLock().unlock();
    }
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
      if (closing || !running)
        return;
      closing = true;
      LOG.info("Timestamp time " + (tNum > 0 ? tTime / tNum : 0));
    } finally {
      runLock.writeLock().unlock();
    }
  }

  public void timeout(long timeout) {
    for (MVTOTransaction<K> ta : transactions.values())
      try {
        ta.timeout(timeout);
      } catch (Throwable t) {
        LOG.error("Failed to timeout transaction " + ta, t);
        t.printStackTrace(System.out);
      }
  }

  public void stopped() {
    runLock.readLock().lock();
    try {
      if (!running)
        return;
    } finally {
      runLock.readLock().unlock();
    }
    runLock.writeLock().lock();
    try {
      running = false;
    } finally {
      runLock.writeLock().unlock();
    }
  }

  @Override
  public void beforeGet(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforeGet(keys);
      }
    }.run(tid);
  }

  @Override
  public void afterGet(TID tid, final Iterable<? extends KeyVersions<K>> kvs)
      throws IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterGet(kvs);
      }
    }.run(tid);
  }

  @Override
  public void failedGet(TID tid, final Iterable<? extends K> keys,
      final Throwable t) throws TransactionAbortedException, IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.failedGet(keys, t);
      }
    }.run(tid);
  }

  @Override
  public void beforePut(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforePut(keys);
      }
    }.run(tid);
  }

  @Override
  public void afterPut(TID tid, final Iterable<? extends K> keys)
      throws IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterPut(keys);
      }
    }.run(tid);
  }

  @Override
  public void failedPut(TID tid, final Iterable<? extends K> keys,
      final Throwable t) throws TransactionAbortedException, IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.failedPut(keys, t);
      }
    }.run(tid);
  }

  @Override
  public TID begin() throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();

      // modify persistent state: acquire ID and log begin
      long before = System.currentTimeMillis();
      long ts = getTimestampManager().acquire();
      timestampTime(System.currentTimeMillis() - before);
      TID id = new TID(ts);
      long sid;
      try {
        sid = getTransactionLog().appendStateTransition(id, STARTED);
      } catch (IOException e) {
        getTimestampManager().release(ts);
        throw e;
      }

      // modify in memory state
      transactions.put(id, new MVTOTransaction<K>(this, id, sid, STARTED));
      return id;
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void commit(final TID tid) throws IOException {
    new IfRunning() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        // clients can retry
        if (closing)
          throw new IOException("Transaction Manager closing");
        ta.commit();
      }
    }.run(tid);
  }

  @Override
  public void abort(final TID tid) throws IOException {
    new IfRunning() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.abort();
      }
    }.run(tid);
  }

  void finalize(final TID tid) throws IOException {
    new IfRunning() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.finalize();
      }
    }.run(tid);
  }

  void scheduleFinalize(final TID tid) {
    pool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          MVTOTransactionManager.this.finalize(tid);
        } catch (Throwable t) {
          // will retry later: commit itself was OK
          LOG.error("Failed to finalize transaction " + this, t);
          t.printStackTrace(System.out);
        }
      }
    });
  }

  abstract class IfRunning {
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

  void checkRunning() throws IOException {
    runLock.readLock().lock();
    try {
      // clients can retry
      if (!running)
        throw new IOException("Transaction Manager not running");
    } finally {
      runLock.readLock().unlock();
    }
  }

  List<K> readLock(Iterable<K> keys) {
    List<K> locked = new ArrayList<K>();
    for (K key : keys) {
      readLock(key);
      locked.add(key);
    }
    return locked;
  }

  void readUnlock(Iterable<K> keys) {
    for (K key: keys)
      readUnlock(key);
  }

  void readLock(K key) {
    ReentrantReadWriteLock lock = keyLocks.get(key);
    if (lock == null) {
      lock = new ReentrantReadWriteLock();
      ReentrantReadWriteLock existing = keyLocks.putIfAbsent(key, lock);
      if (existing != null)
        lock = existing;
    }
    lock.readLock().lock();
  }

  // TODO figure out how to remove key locks safely
  void readUnlock(K key) {
    ReentrantReadWriteLock lock = keyLocks.get(key);
    if (lock != null)
      lock.readLock().unlock();
  }

  void writeLock(K key) {
    ReentrantReadWriteLock lock = keyLocks.get(key);
    if (lock == null) {
      lock = new ReentrantReadWriteLock();
      ReentrantReadWriteLock existing = keyLocks.putIfAbsent(key, lock);
      if (existing != null)
        lock = existing;
    }
    lock.writeLock().lock();
  }

  // TODO figure out how to remove key locks safely
  void writeUnlock(K key) {
    ReentrantReadWriteLock lock = keyLocks.get(key);
    if (lock != null)
      lock.writeLock().unlock();
  }

  MVTOTransaction<K> getTransaction(TID tid) {
    return transactions.get(tid);
  }

  void addReader(K key, long version, MVTOTransaction<K> reader) {
    synchronized (readers) {
      NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = readers
          .get(key);
      if (versions == null) {
        versions = new TreeMap<Long, NavigableSet<MVTOTransaction<K>>>();
        readers.put(key, versions);
      }
      NavigableSet<MVTOTransaction<K>> tas = versions.get(version);
      if (tas == null) {
        tas = new TreeSet<MVTOTransaction<K>>(transactionComparator);
        versions.put(version, tas);
      }
      tas.add(reader);
    }
  }

  // must hold key write lock to call this method
  NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> getReaders(K key) {
    synchronized (readers) {
      return readers.get(key);
    }
  }

  void removeReader(Key key, long version, MVTOTransaction<K> reader) {
    synchronized (readers) {
      NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = readers
          .get(key);
      if (versions == null)
        return;
      NavigableSet<MVTOTransaction<K>> tas = versions.get(version);
      if (tas != null && tas.remove(reader) && tas.isEmpty()
          && versions.remove(version) != null && versions.isEmpty())
        readers.remove(key);
    }
  }

  // must hold key lock to call this method
  void addActiveWriter(K key, MVTOTransaction<K> writer) {
    synchronized (activeWriters) {
      NavigableSet<MVTOTransaction<K>> writers = activeWriters.get(key);
      if (writers == null) {
        writers = new TreeSet<MVTOTransaction<K>>(transactionComparator);
        activeWriters.put(key, writers);
      }
      writers.add(writer);
    }
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
      NavigableSet<MVTOTransaction<K>> writers = activeWriters.get(key);
      if (writers != null && writers.remove(writer) && writers.isEmpty())
        activeWriters.remove(key);
    }
  }

  // garbage collection
  @Override
  public void reclaimed(long ts) {
    runLock.readLock().lock();
    try {
      reclaimLock.writeLock().lock();
      try {
        Long cutoff = null;
        Iterator<MVTOTransaction<K>> it = transactions.values().iterator();

        // reclaim transactions up to reclaim timestamp or first non-finalized
        while (it.hasNext()) {
          MVTOTransaction<K> ta = it.next();
          long tts = ta.getID().getTS();
          // transaction not yet globally reclaimable
          if (timestampManager.compare(tts, ts) > 0) {
            reclaimed = ts;
            break;
          }
          // transaction not yet locally reclaimable
          if (!reclaim(ta)) {
            reclaimed = tts - 1;
            break;
          }
          // try to increment SID
          long sid = ta.getSID();
          if (cutoff == null || transactionLog.compare(cutoff, sid) < 0)
            cutoff = sid;
        }

        /*
         * if we reclaimed a transaction and want to truncate the log, we need
         * to make sure there is not an active transaction with a higher
         * sequence ID. That would be the case if the transaction started later
         * globally, but earlier locally.
         */
        if (cutoff != null) {
          while (it.hasNext()) {
            MVTOTransaction<K> ta = it.next();
            long sid = ta.getSID();
            if (transactionLog.compare(cutoff, sid) > 0)
              cutoff = sid;
          }
          try {
            transactionLog.truncate(cutoff);
          } catch (IOException e) {
            LOG.error("Failed to truncate transaction log", e);
            e.printStackTrace(System.out);
          }
        }
      } finally {
        reclaimLock.writeLock().unlock();
      }
    } finally {
      runLock.readLock().unlock();
    }
  }

  protected boolean reclaim(MVTOTransaction<K> ta) {
    switch (ta.getState()) {
    case STARTED:
    case BLOCKED:
      LOG.warn("found active TAs before oldest timestamp " + ta);
      break;
    case ABORTED:
    case COMMITTED:
      scheduleFinalize(ta.getID());
      break;
    case FINALIZED:
      /*
       * reads can only be cleaned up once all transactions that started before
       * this one have completed
       */
      ta.removeReads();
      transactions.remove(ta.getID());
      return true;
    }
    return false;
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
        transactions.put(tid, new MVTOTransaction<K>(this, tid, sid, STARTED));
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
    case RECORD_TYPE_PUT: {
      if (ta == null)
        return;
      OperationRecord<K> olr = (OperationRecord<K>) record;
      K key = olr.getKey();
      ta.addWrite(key);
      return;
    }
    }
    throw new IllegalStateException("Invalid log record: " + record);
  }

  protected void recover(MVTOTransaction<K> ta) {
    switch (ta.getState()) {
    case BLOCKED:
      throw new IllegalStateException(
          "Transaction in transient state during recovery");
    case STARTED:
      // TODO conditions where we should abort here?
      break;
    case ABORTED:
    case COMMITTED:
      scheduleFinalize(ta.getID());
      break;
    case FINALIZED:
      // nothing to do here, but be happy
      break;
    }
  }
}