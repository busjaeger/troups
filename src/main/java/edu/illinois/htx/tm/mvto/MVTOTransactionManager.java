package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ABORTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.COMMITTED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.KeyVersion;
import edu.illinois.htx.tm.MultiversionTransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.mvto.MVTOTransaction.State;

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
 * TODO:
 * <ol>
 * <li>time out transactions
 * 
 * <li>recover from server restart or crash
 * <ol>
 * <li>write transaction steps into log file
 * <li>reconstruct in-memory data structures from log when server restarts
 * <li>set log check-points to delete old logs and enable faster recovery
 * <ol>
 * 
 * <li>Provide information that enables GC of old versions in the KeyValue store
 * 
 * <li>remove transaction objects from in-memory data structures when they are
 * no longer needed:
 * <ol>
 * <li>All transactions older than the youngest active transaction can be
 * removed from the versions-read index.
 * <li>Aborted transactions can be removed from the transaction map once all
 * written versions have been deleted.
 * <li>Committed transactions can be removed from the transaction map once 1.
 * all deleted versions have been deleted and 2. once all written versions have
 * been deleted, because they have been superseded. (question: is the second
 * condition really necessary)
 * </ol>
 * 
 * <li>reduce synchronization: break up single lock into multiple locks
 * 
 * <li>remove implementation assumptions (see above)
 * 
 * <li>could alter policy to only read committed versions since this would
 * eliminate cascading aborts.
 * 
 * </ol>
 */
public class MVTOTransactionManager<K extends Key> implements
    MultiversionTransactionManager<K> {

  // transactions by transaction ID for efficient direct lookup
  private final Map<Long, MVTOTransaction<K>> transactions;
  // transactions indexed by read versions for efficient conflict detection
  private final Map<Key, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> transactionsByVersionsRead;
  // executor for cleanup operations
  private final ExecutorService executorService;
  // key value store this TM is governing
  private final KeyValueStore<K> keyValueStore;

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore,
      ExecutorService executorService) {
    this.keyValueStore = keyValueStore;
    this.executorService = executorService;
    this.transactions = new HashMap<Long, MVTOTransaction<K>>();
    this.transactionsByVersionsRead = new TreeMap<Key, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public KeyValueStore<K> getKeyValueStore() {
    return keyValueStore;
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
   */
  @Override
  public synchronized void filterReads(long tid,
      Iterable<? extends KeyVersion<K>> versions) {
    MVTOTransaction<K> reader = getActiveTransaction(tid);
    Iterator<? extends KeyVersion<K>> it = versions.iterator();
    while (it.hasNext()) {
      KeyVersion<K> kv = it.next();
      K key = kv.getKey();
      long version = kv.getVersion();
      MVTOTransaction<K> writer = transactions.get(version);

      // if we don't have a TA, assume TA is committed and GC'ed
      if (writer != null) {
        switch (writer.getState()) {
        case ACTIVE:
        case BLOCKED:
          // value not yet committed, add dependency in case writer aborts
          writer.addReadBy(reader);
          reader.addReadFrom(writer);
          // value deleted
          if (writer.hasDeleted(key))
            it.remove();
          break;
        case ABORTED:
          // value written by aborted TA & not yet GC'ed, remove from results
          it.remove();
          continue;
        case COMMITTED:
          // value deleted, but not yet cleaned up
          if (writer.hasDeleted(key))
            it.remove();
          break;
        }
      }

      // TODO log read

      // remember read so we can efficiently check write conflicts later
      reader.addRead(kv);
      index(key, version, reader);

      // older versions not yet cleaned up, remove from results
      while (it.hasNext()) {
        if (kv.getVersion() != it.next().getVersion()) {
          it.previous();
          break;
        }
        it.remove();
      }
    }
  }

  /**
   * 
   * @param tid
   *          transaction timestamp
   * @param key
   * @throws TransactionAbortedException
   */
  @Override
  public synchronized void checkWrite(long tid, K key)
      throws TransactionAbortedException {
    MVTOTransaction<K> writer = getActiveTransaction(tid);
    // TODO log write for recovery
    // record write so we can clean it up if the TA aborts
    writer.addWrite(key);
    checkConflict(writer, key);
  }

  @Override
  public void checkDelete(long tid, K key) throws TransactionAbortedException {
    MVTOTransaction<K> writer = getActiveTransaction(tid);
    // TODO log write for recovery
    // record delete so we can clean it up if the TA aborts, filter it out for
    // reads, and delete it and older versions if TA commits
    writer.addDelete(key);
    checkConflict(writer, key);
  }

  private void checkConflict(MVTOTransaction<K> writer, K key)
      throws TransactionAbortedException {
    // 2. enforce proper time-stamp ordering: abort transaction if needed
    NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = transactionsByVersionsRead
        .get(key);
    if (versions != null) {
      // reduce to versions that were written before this TA started
      for (NavigableSet<MVTOTransaction<K>> readers : versions.headMap(
          writer.getID(), false).values()) {
        // check if any version has been read by a TA that started after this TA
        MVTOTransaction<K> reader = readers.higher(writer);
        if (reader != null) {
          abort(reader.getID());
          throw new TransactionAbortedException("Transaction " + writer.getID()
              + " cannot write, because " + reader
              + " which started after it has already read an older version.");
        }
      }
    }
  }

  @Override
  public synchronized void begin(long tid) {
    MVTOTransaction<K> ta = transactions.get(tid);
    if (ta != null)
      throw new IllegalStateException("Transaction " + tid + " already exists");
    transactions.put(tid, ta);
  }

  @Override
  public synchronized void commit(final long tid)
      throws TransactionAbortedException {
    // only continue if transaction is active
    final MVTOTransaction<K> ta = getTransaction(tid);
    switch (ta.getState()) {
    case ACTIVE:
      break;
    case BLOCKED:
      throw new IllegalStateException("Transaction " + tid + " commit pending");
    case ABORTED:
      throw new TransactionAbortedException("Transaction " + tid
          + " already aborted");
    case COMMITTED:
      return;
    }

    synchronized (ta) {
      while (!ta.getReadFrom().isEmpty()) {
        ta.setState(BLOCKED);
        try {
          // TODO add timeout
          ta.wait();
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        // victim of cascading abort
        if (ta.getState() == State.ABORTED)
          throw new TransactionAbortedException("Transaction " + tid
              + " is victim of cascading abort");
      }
    }

    // TODO log commit
    ta.setState(COMMITTED);

    // notify transactions that read from this one, that it committed (so they
    // don't wait on it)
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          for (Iterator<MVTOTransaction<K>> it = ta.getReadBy().iterator(); it
              .hasNext();) {
            MVTOTransaction<K> readBy = it.next();
            synchronized (readBy) {
              readBy.removeReadFrom(ta);
              if (readBy.getReadFrom().isEmpty())
                readBy.notify();
            }
            it.remove();
          }
        }
        System.out.println("Notified read-from " + tid);
        // TODO log here?
      }
    });

    // schedule clean up of any rows deleted by this transaction
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          // writes were only kept around in case we abort
          for (Iterator<?> it = ta.getWrites().iterator(); it.hasNext();)
            it.remove();
          // deletes now actually need to be applied
          try {
            for (Iterator<K> it = ta.getDeletes().iterator(); it.hasNext();) {
              K key = it.next();
              keyValueStore.deleteVersions(key, tid);
              it.remove();
            }
          } catch (IOException e) {
            e.printStackTrace();
            // TODO handle internal system errors: this causes memory leak
          }
        }
        System.out.println("Cleaned up deletes " + tid);
        // TODO log delete (once should be enough since delete idempotent)
      }
    });

    System.out.println("Committed " + tid);
  }

  @Override
  public synchronized void abort(final long tid) {
    final MVTOTransaction<K> ta = getTransaction(tid);
    boolean isBlocked = false;
    switch (ta.getState()) {
    case ACTIVE:
      break;
    case BLOCKED:
      isBlocked = true;
      break;
    case ABORTED:
      return;
    case COMMITTED:
      throw new IllegalStateException("Transaction " + tid
          + " already committed");
    }

    // TODO log abort
    ta.setState(ABORTED);
    if (isBlocked) {
      synchronized (ta) {
        ta.notify();
      }
    }

    // abort any transactions that read from this one
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          for (Iterator<MVTOTransaction<K>> it = ta.getReadBy().iterator(); it
              .hasNext();) {
            MVTOTransaction<K> readBy = it.next();
            synchronized (readBy) {
              readBy.removeReadFrom(ta);
              abort(readBy.getID());
            }
            it.remove();
          }
        }
      }
    });

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          try {
            for (Iterator<K> it = ta.getMutations().iterator(); it.hasNext();) {
              K key = it.next();
              keyValueStore.deleteVersion(key, tid);
              it.remove();
            }
          } catch (IOException e) {
            e.printStackTrace();
            // TODO handle internal system errors: this causes memory leak
          }
          // TODO log (once should be enough since delete idempotent)
        }
      }
    });

    // TODO schedule clean up of all written rows - or handle as part of GC
  }

  /**
   * Get the transaction object for the given timestamp
   * 
   * @param tid
   * @return
   */
  private MVTOTransaction<K> getTransaction(long tid) {
    MVTOTransaction<K> ta = transactions.get(tid);
    if (ta == null)
      throw new IllegalStateException("Transaction " + tid + "does not exist");
    return ta;
  }

  private MVTOTransaction<K> getActiveTransaction(long tid) {
    MVTOTransaction<K> ta = getTransaction(tid);
    if (ta.getState() != State.ACTIVE)
      throw new IllegalStateException("Transaction " + tid + " not active");
    return ta;
  }

  /**
   * Indexes the given transaction by the key version it read.
   * 
   * @param key
   * @param version
   * @param ta
   */
  private void index(Key key, long version, MVTOTransaction<K> ta) {
    NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = transactionsByVersionsRead
        .get(key);
    if (versions == null)
      transactionsByVersionsRead.put(key,
          versions = new TreeMap<Long, NavigableSet<MVTOTransaction<K>>>());
    NavigableSet<MVTOTransaction<K>> readers = versions.get(version);
    if (readers == null)
      versions.put(version, readers = new TreeSet<MVTOTransaction<K>>());
    readers.add(ta);
  }

}