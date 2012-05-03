package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.TransactionState.ABORTED;
import static edu.illinois.troups.tm.TransactionState.COMMITTED;
import static edu.illinois.troups.tm.TransactionState.FINALIZED;
import static edu.illinois.troups.tm.TransactionState.STARTED;
import static edu.illinois.troups.tm.TransientTransactionState.BLOCKED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tm.NotEnoughVersionsException;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.log.TransactionLog;
import edu.illinois.troups.tsm.TimestampManager;
import edu.illinois.troups.util.perf.ThreadLocalStopWatch;

class MVTOTransaction<K extends Key> {

  private static final Log LOG = LogFactory.getLog(MVTOTransaction.class);

  // immutable state
  protected final MVTOTransactionManager<K, ?> tm;
  protected final TID id;
  // first log sequence number used by this transaction
  protected final long firstSID;

  // mutable state
  // state of this transaction
  protected int state;
  // last time this transaction was accessed: used to time out transaction
  protected long lastTouched;
  // last log sequence number used by this transaction
  protected long lastSID;
  // all versions this transaction has read: used for conflict detection
  private final Map<K, Long> reads;
  // writes this transaction performed: used if transaction aborts
  private final Set<K> writes;
  // transactions this transaction depends on to commit
  private final Set<MVTOTransaction<K>> readFrom;
  // transactions that depend on this transaction to commit
  private final Set<MVTOTransaction<K>> readBy;

  public MVTOTransaction(MVTOTransactionManager<K, ?> tm, TID tid,
      long firstSID, int state) {
    this.tm = tm;
    this.id = tid;
    this.firstSID = firstSID;
    this.state = state;
    this.lastTouched = System.currentTimeMillis();
    this.reads = new HashMap<K, Long>();
    this.writes = new HashSet<K>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
  }

  public final synchronized void commit() throws TransactionAbortedException,
      IOException {
    ThreadLocalStopWatch.start("TA.commit");
    try {
      // check if state transition valid
      if (!shouldCommit())
        return;
      // wait for transactions we read from to complete
      waitForWriters();
      // persist state transition
      getTransactionLog().appendStateTransition(id, COMMITTED);
      // apply in-memory state transition
      setCommitted();
      tm.scheduleFinalize(id);
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  protected boolean shouldCommit() {
    switch (state) {
    case COMMITTED:
    case FINALIZED:
      return false;
    case STARTED:
      return true;
    default:
      throw newISA("commit");
    }
  }

  protected void setCommitted() {
    this.lastTouched = System.currentTimeMillis();
    this.state = COMMITTED;
    /*
     * Inform transactions that read from us that they no longer need to wait on
     * us, since we committed successfully. This has to be done after the commit
     * is recorded, since otherwise we could still fail after telling everyone
     * that we succeeded.
     */
    notifyReaders();
    /*
     * Note that we cannot remove reads here like we do in the abort case. They
     * can only be removed once we are certain that no transactions that started
     * before this one is still active. Therefore, we remove reads when we
     * garbage collect the transaction.
     */
  }

  public final synchronized void abort() throws IOException {
    ThreadLocalStopWatch.start("TA.abort");
    try {
      if (!shouldAbort())
        return;
      getTransactionLog().appendStateTransition(id, ABORTED);
      // note: OK to notify before changing state, since holding monitor
      if (state == BLOCKED)
        notify();
      setAborted();
      tm.scheduleFinalize(id);
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  protected boolean shouldAbort() {
    switch (state) {
    case STARTED:
    case BLOCKED:
      return true;
    case ABORTED:
    case FINALIZED:
      return false;
    default:
      throw newISA("abort");
    }
  }

  // in-memory state transition
  protected void setAborted() {
    this.lastTouched = System.currentTimeMillis();
    this.state = ABORTED;
    /*
     * It does not matter which versions this transactions read, because any
     * changes it made based on those reads will be rolled back. Therefore, we
     * need to remove it's reads as quickly as possible to prevent writers from
     * aborting because of them.
     */
    removeReads();
  }

  // it is actually necessary to finalize on a separate thread, because we are
  // calling back into the region
  public final synchronized void finalize() throws IOException {
    if (!shouldFinalize())
      return;
    if (state == ABORTED)
      rollback();

    releaseTimestamp();
    long sid = getTransactionLog().appendStateTransition(id, FINALIZED);
    setFinalized(sid);
  }

  protected boolean shouldFinalize() {
    switch (state) {
    case COMMITTED:
    case ABORTED:
      return true;
    case FINALIZED:
      return false;
    default:
      throw newISA("finalize");
    }
  }

  protected void releaseTimestamp() throws IOException {
    getTimestampManager().release(id.getTS());
  }

  // in-memory state transition
  protected void setFinalized(long lastSID) {
    this.lastTouched = System.currentTimeMillis();
    state = FINALIZED;
    this.lastSID = lastSID;
  }

  private void rollback() throws IOException {
    /*
     * note: we are not removing this transaction from transactions it read
     * from. These transactions will notify this transaction when they commit or
     * try to abort it when they abort. However, those attempts will be ignored.
     */

    // cascade abort to transactions that read from this one
    for (MVTOTransaction<K> reader : readBy)
      reader.abort();

    // remove any writes in progress and clean up data store

    for (Iterator<K> it = writes.iterator(); it.hasNext();) {
      K key = it.next();
      tm.removeActiveWriter(key, this);
      tm.getKeyValueStore().deleteVersion(key, id.getTS());
      it.remove();
    }
  }

  public synchronized void timeout(long timeout) throws IOException {
    long current = System.currentTimeMillis();
    if (isRunning() && current > lastTouched
        && (lastTouched + timeout) < current) {
      LOG.warn("Aborting timed out transaction " + this + "(last accessed: "
          + lastTouched + ", current: " + current + ")");
      abort();
    }
  }

  public final synchronized void beforeGet(Iterable<? extends K> keys) {
    checkRunning();
    // TODO record reader here
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
  public final void afterGet(int numVersionsRetrieved,
      Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException {
    boolean aborted = false;
    /*
     * First acquire read locks for all the keys we are about to scan: We don't
     * want a writer coming in on a key while we decide which version to pick,
     * since otherwise we may admit the writer even though it would have
     * conflicted with the read we just let it.
     */
    ThreadLocalStopWatch.start("TA.afterGet");
    try {
      List<K> lockedKeys = getKeys(kvs);
      tm.readLock(lockedKeys);
      try {
        synchronized (this) {
          checkRunning();
          List<K> keys = new ArrayList<K>();
          List<Long> versions = new ArrayList<Long>();
          for (KeyVersions<K> kv : kvs) {
            K key = kv.getKey();
            int abortedRemoved = 0;
            for (Iterator<Long> it = kv.getVersions().iterator(); it.hasNext();) {
              long version = it.next();

              /*
               * 1. Get the transaction that wrote this version. If we don't
               * have one, it means the transaction has finished and has been
               * garbage collected.
               */
              MVTOTransaction<K> writer = tm.getTransaction(new TID(version));
              if (writer != null) {
                synchronized (writer) {
                  switch (writer.state) {
                  case ABORTED:
                    /*
                     * transaction has been aborted, but the versions it wrote
                     * have not been cleaned up.
                     */
                    abortedRemoved++;
                    it.remove();
                    continue;
                  case STARTED:
                  case BLOCKED:
                    /*
                     * we are modifying transaction in-memory state here before
                     * logging. This is because we make the decision at this
                     * point to read from this transaction. If we release the
                     * lock on the writer first, the writer could abort shortly
                     * after without knowing that we read from it, so the abort
                     * would not be propagated.
                     */
                    addReadFrom(writer);
                    break;
                  default:
                    break;
                  }
                }
              }

              /*
               * 2. Check if we have admitted a writer that started before this
               * transaction but whose version is not in the result set. If
               * that's the case, we cannot let this reader proceed, because
               * reading an older version would violate the serialization order
               * (if this transaction had already read the older value, the
               * writer would have never been admitted)
               */
              // TODO we need to take order of readers/writers into account
              NavigableSet<MVTOTransaction<K>> writes = tm
                  .getActiveWriters(key);
              if (writes != null) {
                MVTOTransaction<K> lastWrite = writes.lower(this);
                if (lastWrite != null
                    && tm.getTimestampManager().compare(lastWrite.id.getTS(),
                        version) > 0) {
                  tm.readUnlock(lockedKeys);
                  aborted = true;
                  abort();
                  throw new TransactionAbortedException("Read conflict");
                }
              }

              // 3. Add this key and version to the read set
              keys.add(key);
              versions.add(version);

              // 4. remove all older versions of this key from result set
              while (it.hasNext()) {
                it.next();
                it.remove();
              }
            }
            if (abortedRemoved == numVersionsRetrieved)
              throw new NotEnoughVersionsException();
          }

          /*
           * 5. Log the versions we read, so in case the server goes down we can
           * reconstruct the in-memory state to resume processing where we left
           * off. Also
           */
          if (!keys.isEmpty()) {
            getTransactionLog().appendGet(id, keys, versions);
            addGets(keys, versions);
          }
        }
      } finally {
        if (!aborted)
          tm.readUnlock(lockedKeys);
      }
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  protected void addReadFrom(MVTOTransaction<K> writer) {
    writer.readBy.add(this);
    this.readFrom.add(writer);
  }

  // in-memory state transition
  protected void addGets(List<K> keys, List<Long> versions) {
    this.lastTouched = System.currentTimeMillis();
    Iterator<Long> it = versions.iterator();
    for (K key : keys) {
      Long version = it.next();
      reads.put(key, version);
      tm.addRead(key, version, this);
    }
  }

  public final void failedGet(Iterable<? extends K> keys, Throwable t) {
    checkRunning();
  }

  public final void beforePut(Iterable<? extends K> keys) throws IOException {
    ThreadLocalStopWatch.start("TA.beforePut");
    try {
      boolean aborted = false;
      tm.writeLock(keys);
      try {
        synchronized (this) {
          checkRunning();
          List<K> wKeys = new ArrayList<K>();
          for (K key : keys) {
            // enforce proper time-stamp ordering: abort transaction if needed
            NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = tm
                .getReads(key);
            if (versions != null) {
              // reduce to readers that read versions written before this TA
              // started
              for (NavigableSet<MVTOTransaction<K>> readers : versions.headMap(
                  id.getTS(), false).values()) {
                // check if any version has been read by a TA that started after
                // this TA
                MVTOTransaction<K> reader = readers.higher(this);
                if (reader != null) {
                  tm.writeUnlock(key);
                  aborted = true;
                  abort();
                  throw new TransactionAbortedException("Transaction " + id
                      + " write conflict with " + reader.id);
                }
              }
            }
            wKeys.add(key);
          }

          /*
           * Remember any keys written by the transaction, so we can roll the
           * changes back in case it aborts later.
           */
          if (!wKeys.isEmpty()) {
            getTransactionLog().appendPut(id, wKeys);
            addWrites(keys);
            // TODO we need to take order of readers/writers into account
            for (K key : wKeys)
              tm.addActiveWriter(key, this);
          }
        }
      } finally {
        if (!aborted)
          tm.writeUnlock(keys);
      }
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  public final void afterPut(Iterable<? extends K> keys) throws IOException {
    ThreadLocalStopWatch.start("TA.beforePut");
    try {
      tm.writeLock(keys);
      try {
        synchronized (this) {
          checkRunning();
          // TODO we need to take order of readers/writers into account
          for (K key : keys)
            tm.removeActiveWriter(key, this);
        }
      } finally {
        tm.writeUnlock(keys);
      }
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  public final void failedPut(Iterable<? extends K> keys, Throwable t)
      throws IOException {
    afterPut(keys);
  }

  // in-memory state transition
  protected void addWrites(Iterable<? extends K> keys) {
    this.lastTouched = System.currentTimeMillis();
    for (K key : keys)
      writes.add(key);
  }

  // ----------------------------------------------------------------------
  // getter/setters for transaction state
  // used by state transitions methods and recovery process
  // ----------------------------------------------------------------------

  protected void checkRunning() {
    if (!isRunning())
      throw newISA("access");
  }

  protected boolean isRunning() {
    return state == STARTED;
  }

  protected TransactionLog<K, ?> getTransactionLog() {
    return tm.getTransactionLog();
  }

  protected TimestampManager getTimestampManager() {
    return tm.getTimestampManager();
  }

  TID getID() {
    return this.id;
  }

  long getFirstSID() {
    return this.firstSID;
  }

  synchronized long getLastSID() {
    return lastSID;
  }

  synchronized int getState() {
    return state;
  }

  // blocks until read-from transactions complete
  synchronized void waitForWriters() throws IOException {
    ThreadLocalStopWatch.start("TA.beforePut");
    try {
      while (!readFrom.isEmpty()) {
        state = BLOCKED;
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        // we released the lock, so state may have changed
        switch (state) {
        case ABORTED:
          // 1. the read-from transaction aborted and cascaded its abort
          throw new TransactionAbortedException("Transaction " + id
              + " is victim of cascading abort");
        case STARTED:
          // 2. the transaction manager was shut down and interrupted the commit
          throw new IOException("Commit interrupted");// IOException means:
                                                      // retry
        case BLOCKED:
          break;
        case COMMITTED:
        case FINALIZED:
          // 3. we should never get into one of these states
          throw newISA("doCommit");
        }
      }
    } finally {
      ThreadLocalStopWatch.stop();
    }
  }

  synchronized void notifyReaders() {
    for (MVTOTransaction<K> ta : readBy) {
      synchronized (ta) {
        if (ta.readFrom.remove(this) && ta.readFrom.isEmpty())
          ta.notify();
      }
    }
  }

  synchronized void unblock() {
    if (state == BLOCKED) {
      state = STARTED;
      notifyAll();// notify should be enough, but be safe
    }
  }

  synchronized void removeReads() {
    for (Entry<K, Long> read : reads.entrySet()) {
      K key = read.getKey();
      tm.removeRead(key, read.getValue(), this);
    }
  }

  IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MVTOTransaction))
      return false;
    MVTOTransaction<K> ota = (MVTOTransaction<K>) obj;
    return id.equals(ota.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id + "(" + state + ")";
  }

  private static <K extends Key> List<K> getKeys(
      Iterable<? extends KeyVersions<K>> kvs) {
    List<K> list = new ArrayList<K>();
    for (KeyVersions<K> kv : kvs)
      list.add(kv.getKey());
    return list;
  }

}