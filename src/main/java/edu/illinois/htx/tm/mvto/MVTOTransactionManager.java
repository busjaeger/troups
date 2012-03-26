package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ABORTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.COMMITTED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    MultiversionTransactionManager<K>, Runnable {

  // immutable state
  // executor for async operations
  private final ScheduledExecutorService executorService;
  // key value store this TM is governing
  private final KeyValueStore<K> keyValueStore;

  // mutable state
  // transactions by transaction ID for efficient direct lookup
  private final NavigableMap<Long, MVTOTransaction<K>> transactions;

  // mutable state cached for efficiency
  // transactions indexed by read versions for efficient conflict detection
  private final Map<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>> transactionsByVersionsRead;
  // last uncommitted transaction id
  private long firstActive;

  public MVTOTransactionManager(KeyValueStore<K> keyValueStore,
      ScheduledExecutorService executorService) {
    this.keyValueStore = keyValueStore;
    this.executorService = executorService;
    this.executorService.scheduleAtFixedRate(this, 5, 1, TimeUnit.SECONDS);
    this.firstActive = 0L;
    this.transactions = new TreeMap<Long, MVTOTransaction<K>>();
    this.transactionsByVersionsRead = new HashMap<K, NavigableMap<Long, NavigableSet<MVTOTransaction<K>>>>();
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public KeyValueStore<K> getKeyValueStore() {
    return keyValueStore;
  }

  @Override
  public long getFirstActiveTID() {
    return firstActive;
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
    if (it.hasNext()) {
      KeyVersion<K> kv = it.next();
      outer: while (true) {
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

        // remember read for conflict detection
        addRead(reader, key, version);

        // older versions not yet cleaned up, remove from results
        while (it.hasNext()) {
          KeyVersion<K> next = it.next();
          if (kv.getVersion() != next.getVersion()) {
            kv = next;
            continue outer;
          }
          it.remove();
        }
        // no further elements
        break;
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
  public synchronized void checkWriteConflict(long tid, K key, boolean isDelete)
      throws TransactionAbortedException {
    MVTOTransaction<K> writer = getActiveTransaction(tid);
    // TODO log write for recovery

    // record write so we can clean it up if the TA aborts. If it is a delete,
    // we also use this information to filter deleted versions from reads
    // results and to delete values from the underlying data store when the
    // transaction commits
    writer.addWrite(key, isDelete);

    // enforce proper time-stamp ordering: abort transaction if needed
    if (hasYoungerReaderOfOlderWriter(key, writer)) {
      abort(writer.getID());
      throw new TransactionAbortedException(
          "Transaction "
              + writer.getID()
              + " cannot write, because a newer transaction has already read an older value.");
    }
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
          Iterator<MVTOTransaction<K>> it = ta.getReadBy().iterator();
          while (it.hasNext()) {
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
          // clean up writes and deletes
          try {
            for (Iterator<Entry<K, Boolean>> it = ta.getWrites().entrySet()
                .iterator(); it.hasNext();) {
              Entry<K, Boolean> e = it.next();
              // remove deletes from data store so we don't have to keep this
              // transaction around to filter reads
              if (e.getValue())
                keyValueStore.deleteVersions(e.getKey(), tid);
              // writes were only kept around in case this TA aborts
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
    boolean wasBlocked = false;
    switch (ta.getState()) {
    case ACTIVE:
      break;
    case BLOCKED:
      wasBlocked = true;
      break;
    case ABORTED:
      return;
    case COMMITTED:
      throw new IllegalStateException("Transaction " + tid
          + " already committed");
    }

    // TODO log abort
    ta.setState(ABORTED);
    if (wasBlocked) {
      synchronized (ta) {
        ta.notify();
      }
    }

    // This TA should no longer cause write conflicts, since it's aborted
    removeReads(ta);

    // abort any transactions that read from this one
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          // we can forget who we read from: will never commit and therefore
          // never need to wait on the TAs we read from to complete
          Iterator<MVTOTransaction<K>> it = ta.getReadFrom().iterator();
          while (it.hasNext())
            it.remove();

          // cascade abort to transactions that read from this one
          // TODO: can this really be done on a separate thread?
          it = ta.getReadBy().iterator();
          while (it.hasNext()) {
            MVTOTransaction<K> readBy = it.next();
            synchronized (readBy) {
              readBy.removeReadFrom(ta);
              abort(readBy.getID());
            }
            it.remove();
          }

          // TODO log cascading complete (or can this be derived from log?)
        }
      }
    });

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        synchronized (MVTOTransactionManager.this) {
          try {
            Iterator<K> it = ta.getWrites().keySet().iterator();
            while (it.hasNext()) {
              K key = it.next();
              keyValueStore.deleteVersion(key, tid);
              it.remove();
            }
          } catch (IOException e) {
            e.printStackTrace();
            // TODO handle internal errors (e.g. retry) to avoid resource leaks
          }
          // TODO log undo complete (once is enough since delete is idempotent)
        }
      }
    });
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
      transactions.put(tid, ta = new MVTOTransaction<K>(tid));
    return ta;
  }

  private MVTOTransaction<K> getActiveTransaction(long tid) {
    MVTOTransaction<K> ta = getTransaction(tid);
    if (ta.getState() != State.ACTIVE)
      throw new IllegalStateException("Transaction " + tid + " not active");
    return ta;
  }

  /**
   * Adds the read version to the transaction, but also indexes it for faster
   * lookup during write conflict detection.
   * 
   * @param ta
   * @param key
   * @param version
   */
  private void addRead(MVTOTransaction<K> ta, K key, long version) {
    // add to TA so we can remove it later
    ta.addRead(key, version);
    // now index it for efficient retrieval
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

  /**
   * Returns true if a transaction that started after the given writer read a
   * version of the key written by a transaction that started before the writer.
   * 
   * @param key
   * @param writer
   * @return
   */
  private boolean hasYoungerReaderOfOlderWriter(K key, MVTOTransaction<K> writer) {
    NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = transactionsByVersionsRead
        .get(key);
    if (versions != null) {
      // reduce to versions that were written before this TA started
      for (NavigableSet<MVTOTransaction<K>> readers : versions.headMap(
          writer.getID(), false).values()) {
        // check if any version has been read by a TA that started after this TA
        return readers.higher(writer) != null;
      }
    }
    return false;
  }

  /**
   * Removes all reads stored in this transaction from the index and from the
   * transaction.
   * 
   * @param ta
   */
  private void removeReads(MVTOTransaction<K> ta) {
    Iterator<Entry<K, Long>> it = ta.getReads().entrySet().iterator();
    while (it.hasNext()) {
      Entry<K, Long> e = it.next();
      // first remove read from index
      NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = transactionsByVersionsRead
          .get(e.getKey());
      if (versions != null) {
        NavigableSet<MVTOTransaction<K>> tas = versions.get(e.getValue());
        if (tas != null) {
          tas.remove(ta);
          if (tas.isEmpty()) {
            versions.remove(e.getValue());
            if (versions.isEmpty())
              transactionsByVersionsRead.remove(e.getKey());
          }
        }
      }
      // then remove read from transaction
      it.remove();
    }
  }

  // garbage collection
  @Override
  public void run() {
    synchronized (this) {
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
          if (ta.getReadBy().isEmpty() && ta.getWrites().isEmpty())
            transactions.remove(tid);
          break;
        case COMMITTED:
          // reads can only be cleaned up once all transactions that started
          // before this one have completed
          if (!foundActive) {
            // we may have cleaned up reads on an ealier pass
            removeReads(ta);
            if (ta.getReadBy().isEmpty() && ta.getWrites().isEmpty())
              transactions.remove(tid);
          }
          break;
        }
      }
    }
  }
}