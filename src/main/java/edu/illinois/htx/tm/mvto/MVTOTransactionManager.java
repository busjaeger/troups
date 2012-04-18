package edu.illinois.htx.tm.mvto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.KeyValueStoreObserver;
import edu.illinois.htx.tm.KeyVersions;
import edu.illinois.htx.tm.Log;
import edu.illinois.htx.tm.LogRecord;
import edu.illinois.htx.tm.LogRecord.Type;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.TransactionManager;
import edu.illinois.htx.tm.mvto.MVTOTransaction.State;
import edu.illinois.htx.tsm.TimestampManager;
import edu.illinois.htx.tsm.TimestampManager.TimestampListener;

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
 * <li>think through crash recovery
 * 
 * <li>remove implementation assumptions (see above)
 * 
 * <li>support alternate policy to only read committed versions (to eliminate
 * cascading aborts)
 * 
 * </ol>
 */
public class MVTOTransactionManager<K extends Key, R extends LogRecord<K>>
    implements KeyValueStoreObserver<K>, TimestampListener, TransactionManager {

  // immutable state
  // key value store this TM is governing
  private final KeyValueStore<K> keyValueStore;
  // transaction log
  private final Log<K, R> transactionLog;
  // timestamp oracle
  protected final TimestampManager timestampManager;

  // mutable state
  // transactions by transaction ID for efficient direct lookup
  private final NavigableMap<Long, MVTOTransaction<K>> transactions;
  // TAs indexed by key and versions read for efficient conflict detection
  private final Map<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> readers;
  // TAs indexed by key currently being written for efficient conflict detection
  private final Map<K, NavigableSet<MVTOTransaction<K>>> writers;
  // lock protect the previous two conflict detection data structures
  private final ConcurrentMap<Key, Lock> keyLocks = new ConcurrentHashMap<Key, Lock>();
  // flag to indicate whether this TM is running
  protected boolean running;
  // guards the running flag
  protected ReadWriteLock runLock = new ReentrantReadWriteLock();

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore, Log<K, R> log,
      TimestampManager timestampManager) {
    this.keyValueStore = keyValueStore;
    this.transactionLog = log;
    this.timestampManager = timestampManager;
    this.transactions = new TreeMap<Long, MVTOTransaction<K>>();
    this.readers = new HashMap<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
    this.writers = new HashMap<K, NavigableSet<MVTOTransaction<K>>>();
  }

  public KeyValueStore<K> getKeyValueStore() {
    return keyValueStore;
  }

  public Log<K, R> getTransactionLog() {
    return transactionLog;
  }

  public TimestampManager getTimestampManager() {
    return timestampManager;
  }

  public void start() throws IOException {
    runLock.writeLock().lock();
    try {
      if (running)
        return;
      recover();
      timestampManager.addLastDeletedTimestampListener(this);
      running = true;
    } finally {
      runLock.writeLock().unlock();
    }
  }

  public synchronized void stop(boolean abort) {

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
    } finally {
      runLock.writeLock().unlock();
    }
  }

  @Override
  public void beforeRead(long tid, Iterable<? extends K> keys) {
    // TODO need to keep finalized transactions around until all readers
    // that started before they were finalized have completed
  }

  /**
   * Removes any KeyValues from the list that have been overwritten by newer
   * versions or were written by aborted transactions.
   * 
   * @param tid
   *          transaction time-stamp
   * @param kvs
   *          MUST BE: sorted by key with newer versions ordered before older
   *          versions and all for the same row.
   * @throws TransactionAbortedException
   */
  @Override
  public void afterRead(final long tid,
      final Iterable<? extends KeyVersions<K>> kvs) throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterRead(kvs);
      }
    }.run(tid);
  }

  @Override
  public synchronized void beforeWrite(final long tid, final boolean isDelete,
      final Iterable<? extends K> keys) throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.beforeWrite(isDelete, keys);
      }
    }.run(tid);
  }

  @Override
  public synchronized void afterWrite(final long tid, final boolean isDelete,
      final Iterable<? extends K> keys) throws IOException {
    new WithReadLock() {
      @Override
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.afterWrite(isDelete, keys);
      }
    }.run(tid);
  }

  @Override
  public long begin() throws IOException {
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
  public void commit(final long tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.commit();
        System.out.println("Committed " + tid);
      }
    }.run(tid);
  }

  @Override
  public void abort(final long tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.abort();
        System.out.println("Aborted " + tid);
      }
    }.run(tid);
  }

  abstract class WithReadLock {
    void run(long tid) throws IOException {
      runLock.readLock().lock();
      try {
        checkRunning();
        MVTOTransaction<K> ta = getTransaction(tid);
        if (ta == null)
          throw new IllegalStateException("Transaction " + tid
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

  /**
   * Get the transaction object for the given timestamp
   * 
   * @param tid
   * @return
   */
  MVTOTransaction<K> getTransaction(long tid) {
    synchronized (transactions) {
      return transactions.get(tid);
    }
  }

  void addTransaction(MVTOTransaction<K> ta) {
    synchronized (transactions) {
      transactions.put(ta.getID(), ta);
    }
  }

  MVTOTransaction<K> removeTransaction(MVTOTransaction<K> ta) {
    synchronized (transactions) {
      return transactions.remove(ta.getID());
    }
  }

  Iterable<MVTOTransaction<K>> getTransactions() {
    List<MVTOTransaction<K>> snapshot;
    synchronized (transactions) {
      snapshot = new ArrayList<MVTOTransaction<K>>(transactions.values());
    }
    return snapshot;
  }

  // must be called with key lock held
  NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> getReaders(K key) {
    synchronized (readers) {
      return readers.get(key);
    }
  }

  /**
   * Indexes the read for efficient conflict detection. Caller must hold key
   * lock.
   * 
   * @param reader
   * @param key
   * @param version
   */
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
      versions.put(version, readers = new TreeSet<MVTOTransaction<K>>());
    readers.add(reader);
  }

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
  void addWriter(K key, MVTOTransaction<K> writer) {
    NavigableSet<MVTOTransaction<K>> writes;
    synchronized (writers) {
      writes = writers.get(key);
      if (writes == null)
        writers.put(key, writes = new TreeSet<MVTOTransaction<K>>());
    }
    writes.add(writer);
  }

  // must hold key lock to call this method
  NavigableSet<MVTOTransaction<K>> getWriters(K key) {
    synchronized (writers) {
      return writers.get(key);
    }
  }

  // must hold key lock to call this method
  void removeWriter(Key key, MVTOTransaction<K> writer) {
    synchronized (writers) {
      NavigableSet<MVTOTransaction<K>> writes = writers.get(key);
      if (writes != null) {
        writes.remove(writer);
        if (writes.isEmpty())
          writers.remove(key);
      }
    }
  }

  // garbage collection
  @Override
  public void deleted(long ts) {
    for (MVTOTransaction<K> ta : getTransactions()) {
      long tid = ta.getID();
      if (tid >= ts)
        return;
      switch (ta.getState()) {
      case CREATED:
        removeTransaction(ta);
        break;
      case ACTIVE:
      case BLOCKED:
        System.out.println("WARNING: found active TAs before oldest timestamp "
            + tid);
        try {
          ta.abort();
        } catch (IOException e) {
          // TODO
          e.printStackTrace();
        }
        if (ta.getState() == State.FINALIZED)
          removeTransaction(ta);
        return;
      case ABORTED:
      case COMMITTED:
        break;
      case FINALIZED:
        /*
         * reads can only be cleaned up once all transactions that started
         * before this one have completed
         */
        ta.removeReads();
        removeTransaction(ta);
        break;
      }
    }
  }

  /**
   * for now assumes proper region shutdown
   * 
   * @throws IOException
   */
  // TODO think through recovery
  // TODO factor out 'JOIN' and 'PREPARED' properly
  private void recover() throws IOException {
    for (LogRecord<K> record : transactionLog.recover()) {
      long tid = record.getTID();
      Type type = record.getType();
      // ignore transactions that began before the log save-point
      MVTOTransaction<K> ta = transactions.get(tid);
      if (ta == null && !(type == Type.BEGIN || type == Type.JOIN))
        continue;

      switch (record.getType()) {
      case BEGIN: {
        transactions.put(tid, ta = new MVTOTransaction<K>(this));
        ta.setID(tid);
        ta.setState(State.ACTIVE);
        break;
      }
      case READ: {
        K key = record.getKey();
        long version = record.getVersion();
        MVTOTransaction<K> writer = transactions.get(version);
        if (writer != null) {
          switch (writer.getState()) {
          case ACTIVE:
          case BLOCKED:
            // value not yet committed, add dependency in case writer aborts
            writer.addReadBy(ta);
            ta.addReadFrom(writer);
            break;
          case CREATED:// TODO created violates loop invariants
          case ABORTED: // TODO abort violates loop invariants
          case COMMITTED:
          case FINALIZED:
            break;
          }
        }
        // remember read for conflict detection
        ta.addRead(key, version);
        addReader(key, version, ta);
        break;
      }
      case WRITE:
      case DELETE: {
        ta.addWrite(record.getKey(), record.getType() == Type.DELETE);
        break;
      }
      case ABORT: {
        ta.removeReads();
        ta.setState(State.ABORTED);
        break;
      }
      case COMMIT: {
        ta.setState(State.COMMITTED);
        break;
      }
      case FINALIZE: {
        switch (ta.getState()) {
        case ABORTED:
        case COMMITTED:
          ta.removeReadBy();
          break;
        default:
          // this shouldn't happen
        }
      }
      case JOIN:
        recoverJoin(record);
        break;
      case PREPARE:
        recoverPrepare(record);
        break;
      }
    }
  }

  protected void recoverJoin(LogRecord<K> record) {
    throw new IllegalStateException("Invalid log record "+record);
  }

  protected void recoverPrepare(LogRecord<K> record) {
    throw new IllegalStateException("Invalid log record "+record);
  }

}