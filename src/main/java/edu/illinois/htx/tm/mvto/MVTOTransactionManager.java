package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ABORTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.COMMITTED;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.KeyVersion;
import edu.illinois.htx.tm.MultiversionTransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.TransactionLog;
import edu.illinois.htx.tm.TransactionLog.Record.Type;

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
 * <li>change filterReads interface
 * 
 * <li>recover from crash
 * 
 * <li>time out transactions
 * 
 * <li>remove implementation assumptions (see above)
 * 
 * <li>support alternate policy to only read committed versions (to eliminate
 * cascading aborts)
 * 
 * </ol>
 */
public class MVTOTransactionManager<K extends Key> implements
    MultiversionTransactionManager<K>, Runnable {

  // immutable state
  // executor for async operations
  private final ScheduledExecutorService executorService;
  // key value store this TM is governing
  private final KeyValueStore<K> keyValueStore;
  // transaction log
  private final TransactionLog<K> transactionLog;

  // mutable state
  // transactions by transaction ID for efficient direct lookup
  private final NavigableMap<Long, MVTOTransaction<K>> transactions;
  // last uncommitted transaction id
  private long firstActive;
  // TAs indexed by key and versions read for efficient conflict detection
  private final Map<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> readers;
  // TAs indexed by key currently being written for efficient conflict detection
  private final Map<K, NavigableSet<MVTOTransaction<K>>> writers;
  // lock protect the previous two conflict detection data structures
  private final ConcurrentMap<Key, Lock> keyLocks = new ConcurrentHashMap<Key, Lock>();
  // flag to indicate whether this TM is running
  private boolean running;
  // guards the running flag
  private ReadWriteLock runLock = new ReentrantReadWriteLock();

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore,
      ScheduledExecutorService executorService, TransactionLog<K> transactionLog) {
    this.keyValueStore = keyValueStore;
    this.executorService = executorService;
    this.transactionLog = transactionLog;
    this.transactions = new TreeMap<Long, MVTOTransaction<K>>();
    this.readers = new HashMap<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
    this.writers = new HashMap<K, NavigableSet<MVTOTransaction<K>>>();
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public KeyValueStore<K> getKeyValueStore() {
    return keyValueStore;
  }

  public TransactionLog<K> getLog() {
    return transactionLog;
  }

  @Override
  public long getFirstActiveTID() {
    return firstActive;
  }

  public boolean isRunning() {
    runLock.readLock().lock();
    try {
      return running;
    } finally {
      runLock.readLock().unlock();
    }
  }

  public void start() {
    runLock.writeLock().lock();
    try {
      replay();
      this.executorService.scheduleAtFixedRate(this, 5, 1, TimeUnit.SECONDS);
      running = true;
    } finally {
      runLock.writeLock().unlock();
    }
  }

  public synchronized void stop() {
    runLock.writeLock().lock();
    try {
      running = false;
      run();
      for (MVTOTransaction<K> ta : transactions.values())
        ta.unblock();
    } finally {
      runLock.writeLock().unlock();
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
  public void filterReads(final long tid,
      final Iterable<? extends KeyVersion<K>> versions)
      throws TransactionAbortedException {
    new Operation<TransactionAbortedException>() {
      @Override
      void execute(MVTOTransaction<K> ta) throws TransactionAbortedException {
        ta.afterRead(versions);
      }
    }.run(tid);
  }

  @Override
  public synchronized void preWrite(final long tid, final boolean isDelete,
      final Iterable<? extends K> keys) throws TransactionAbortedException {
    new Operation<TransactionAbortedException>() {
      @Override
      void execute(MVTOTransaction<K> ta) throws TransactionAbortedException {
        ta.beforeWrite(isDelete, keys);
      }
    }.run(tid);
  }

  @Override
  public synchronized void postWrite(final long tid, final boolean isDelete,
      final Iterable<? extends K> keys) throws TransactionAbortedException {
    new Operation<TransactionAbortedException>() {
      @Override
      void execute(MVTOTransaction<K> ta) throws TransactionAbortedException {
        ta.afterWrite(isDelete, keys);
      }
    }.run(tid);
  }

  public synchronized void begin(final long tid) {
    new Operation<RuntimeException>() {
      void execute(long tid) {
        addTransaction(tid);
      };
    }.run(tid);
  }

  @Override
  public void commit(final long tid) throws TransactionAbortedException {
    new Operation<TransactionAbortedException>() {
      void execute(MVTOTransaction<K> ta) throws TransactionAbortedException {
        ta.commit();
        System.out.println("Committed " + tid);
      }
    }.run(tid);
  }

  @Override
  public void abort(final long tid) {
    new Operation<RuntimeException>() {
      void execute(MVTOTransaction<K> ta) {
        ta.abort();
        System.out.println("Aborted " + tid);
      }
    }.run(tid);
  }

  abstract class Operation<E extends Exception> {
    void run(long tid) throws E {
      runLock.readLock().lock();
      try {
        if (!isRunning())
          throw new IllegalStateException("Transaction Manager stopped");
        execute(tid);
      } finally {
        runLock.readLock().unlock();
      }
    }

    void execute(long tid) throws E {
      MVTOTransaction<K> ta = getTransaction(tid);
      if (ta == null)
        throw new IllegalStateException("Transaction " + tid
            + " does not exist");
      execute(ta);
    }

    void execute(MVTOTransaction<K> ta) throws E {
      // overwrite
    }
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

  void addTransaction(long tid) {
    synchronized (transactions) {
      if (transactions.get(tid) != null)
        throw new IllegalStateException("Transaction " + tid
            + " already started");
      if (!transactions.isEmpty() && transactions.lastEntry().getKey() > tid)
        throw new IllegalArgumentException("Transaction ID " + tid
            + " has already been used");
      MVTOTransaction<K> ta = new MVTOTransaction<K>(tid,
          MVTOTransactionManager.this);
      ta.begin();
      transactions.put(tid, ta);
    }
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
  public void run() {
    synchronized (transactions) {
      boolean foundActive = false;
      for (Iterator<MVTOTransaction<K>> it = transactions.values().iterator(); it
          .hasNext();) {
        MVTOTransaction<K> ta = it.next();
        long tid = ta.getID();
        switch (ta.getState()) {
        case ACTIVE:
        case BLOCKED:
          if (!foundActive) {
            foundActive = true;
            firstActive = tid;
          }
          break;
        case ABORTED:
          // if readBy is empty (all TAs that read from it are aborted) and
          // writes are empty (all written versions have been deleted)
          // then we no longer need to keep this transaction around
          if (ta.isFinalized())
            it.remove();
          break;
        case COMMITTED:
          // reads can only be cleaned up once all transactions that started
          // before this one have completed
          if (!foundActive) {
            ta.removeReads();
            if (ta.isFinalized())
              it.remove();
          }
          break;
        }
      }
    }
  }

  /**
   * for now assumes proper region shutdown
   */
  private void replay() {
    boolean first = true;
    firstActive = 0;
    for (TransactionLog.Record<K> record : transactionLog.read()) {
      long tid = record.getTID();
      // ignore transactions that began before the log save-point
      MVTOTransaction<K> ta = transactions.get(tid);
      if (ta == null && record.getType() != Type.BEGIN)
        continue;

      switch (record.getType()) {
      case BEGIN: {
        if (first) {
          firstActive = tid;
          first = false;
        }
        transactions.put(tid, new MVTOTransaction<K>(tid, this));
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
          case ABORTED:
            // TODO this violates invariants
            break;
          case COMMITTED:
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
        ta.setState(ABORTED);
        break;
      }
      case COMMIT: {
        ta.setState(COMMITTED);
        break;
      }
      case FINALIZE: {
        switch (ta.getState()) {
        case ABORTED:
        case COMMITTED:
          Iterator<MVTOTransaction<K>> it = ta.getReadBy().iterator();
          while (it.hasNext()) {
            MVTOTransaction<K> readBy = it.next();
            readBy.removeReadFrom(ta);
            it.remove();
          }
          break;
        default:
          // this shouldn't happen
        }
      }
      }
    }
  }
}