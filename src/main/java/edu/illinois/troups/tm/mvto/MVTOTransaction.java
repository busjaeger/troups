package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.TransactionState.ABORTED;
import static edu.illinois.troups.tm.TransactionState.COMMITTED;
import static edu.illinois.troups.tm.TransactionState.FINALIZED;
import static edu.illinois.troups.tm.TransactionState.STARTED;
import static edu.illinois.troups.tm.mvto.TransientTransactionState.BLOCKED;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.log.TransactionLog;
import edu.illinois.troups.tsm.TimestampManager;

class MVTOTransaction<K extends Key> {

  private static final Log LOG = LogFactory.getLog(MVTOTransaction.class);

  // immutable state
  protected final MVTOTransactionManager<K, ?> tm;
  protected final TID id;
  protected final long sid;

  // mutable state
  protected int state;
  protected long lastTouched;
  private final Map<K, Long> reads;
  private final Set<K> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final Set<MVTOTransaction<K>> readBy;

  public MVTOTransaction(MVTOTransactionManager<K, ?> tm, TID tid, long sid,
      int state) {
    this.tm = tm;
    this.id = tid;
    this.sid = sid;
    this.state = state;
    this.lastTouched = System.currentTimeMillis();
    this.reads = new HashMap<K, Long>();
    this.writes = new HashSet<K>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
  }

  public final synchronized void commit() throws TransactionAbortedException,
      IOException {
    if (!shouldCommit())
      return;
    waitForReadFrom();
    getTransactionLog().appendStateTransition(id, COMMITTED);
    setCommitted();
    tm.scheduleFinalize(id);
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

  // in-memory state transition
  protected void setCommitted() {
    this.lastTouched = System.currentTimeMillis();
    this.state = COMMITTED;
  }

  public final synchronized void abort() throws IOException {
    if (!shouldAbort())
      return;
    getTransactionLog().appendStateTransition(id, ABORTED);
    if (state == BLOCKED)
      notify();
    setAborted();
    tm.scheduleFinalize(id);
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
    // This TA should no longer cause write conflicts, since it's aborted
    removeReads();
  }

  public synchronized void timeout(long timeout) throws IOException {
    long current = System.currentTimeMillis();
    if (isRunning() && current > lastTouched
        && (lastTouched + timeout) < current) {
      LOG.warn("Aborting transaction " + this + " lastTouched " + lastTouched
          + " at " + current);
      abort();
    }
  }

  // it is actually necessary to finalize on a separate thread, because we are
  // calling back into the region
  public final synchronized void finalize() throws IOException {
    if (!shouldFinalize())
      return;
    switch (state) {
    case COMMITTED:
      finalizeCommit();
      break;
    case ABORTED:
      finalizeAbort();
      break;
    }
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

  /*
   * After a transaction commits, we still need to notify any waiting readers
   * and permanently remove deleted cells. This could be done on a separate
   * thread, since it is decoupled from the commit operation.
   */
  private synchronized void finalizeCommit() throws IOException {
    if (!shouldFinalize())
      return;

    /*
     * notify transactions that read from this one, that it committed (so they
     * don't wait on it): this has to be done after commit, since we don't want
     * to notify and then fail before commit.
     */
    // note: doing this up-front to unblock as quickly as possible
    notifyReadBy();

    // logging once is sufficient, since delete operation idempotent
    releaseTimestamp();
    getTransactionLog().appendStateTransition(id, FINALIZED);
    setCommitFinalized();
  }

  // in-memory state transition
  protected void setCommitFinalized() {
    this.lastTouched = System.currentTimeMillis();
    notifyReadBy();
    state = FINALIZED;
  }

  private synchronized void finalizeAbort() throws IOException {
    if (!shouldFinalize())
      return;

    // cascade abort to transactions that read from this one
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      MVTOTransaction<K> readBy = it.next();
      readBy.abort();
      it.remove();
    }

    // remove any writes in progress and clean up data store
    for (Iterator<K> it = writes.iterator(); it.hasNext();) {
      K key = it.next();
      tm.removeActiveWriter(key, this);
      tm.getKeyValueStore().deleteVersion(key, id.getTS());
      it.remove();
    }

    releaseTimestamp();
    getTransactionLog().appendStateTransition(id, FINALIZED);
    setAbortFinalized();
  }

  // in-memory state transition
  protected void setAbortFinalized() {
    this.lastTouched = System.currentTimeMillis();
    state = FINALIZED;
  }

  protected void releaseTimestamp() throws IOException {
    long before = System.currentTimeMillis();
    getTimestampManager().release(id.getTS());
    tm.timestampTime(System.currentTimeMillis() - before);
  }

  public final synchronized void beforeGet(Iterable<? extends K> keys) {
    checkRunning();
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
  public final void afterGet(Iterable<? extends KeyVersions<K>> kvs)
      throws IOException {
    for (KeyVersions<K> kv : kvs) {
      K key = kv.getKey();
      tm.readLock(key);
      try {
        synchronized (this) {
          checkRunning();
          for (Iterator<Long> it = kv.getVersions().iterator(); it.hasNext();) {
            long version = it.next();

            /*
             * 1. filter out aborted and deleted versions
             */
            MVTOTransaction<K> writer = tm.getTransaction(new TID(version));
            // if we don't have the writer, assume it is has been GC'ed
            if (writer != null) {
              synchronized (writer) {
                switch (writer.state) {
                case ABORTED:
                  it.remove();
                  continue;
                default:
                  break;
                }
              }
            }

            /*
             * 2. Check if we have admitted a writer that started before this
             * transaction but whose version is not in the result set. If that's
             * the case, we cannot let this reader proceed, because reading an
             * older version would violate the serialization order (if this
             * transaction had already read the older value, the writer would
             * have never been admitted)
             */
            NavigableSet<MVTOTransaction<K>> writes = tm.getActiveWriters(key);
            if (writes != null) {
              MVTOTransaction<K> lastWrite = writes.lower(this);
              if (lastWrite != null
                  && tm.getTimestampManager().compare(lastWrite.id.getTS(),
                      version) > 0) {
                tm.readUnlock(key);
                abort();
                throw new TransactionAbortedException("Read conflict");
              }
            }

            // 3. remember read for conflict detection
            getTransactionLog().appendGet(id, key, version);
            addGet(key, version);

            // 4. remove all older versions from result set
            while (it.hasNext()) {
              it.next();
              it.remove();
            }
          }
        }
      } finally {
        try {
          tm.readUnlock(key);
        } catch (IllegalMonitorStateException e) {
          // ignore
        }
      }
    }
  }

  // in-memory state transition
  protected void addGet(K key, long version) {
    this.lastTouched = System.currentTimeMillis();
    MVTOTransaction<K> writer = tm.getTransaction(new TID(version));
    // if value is not yet committed, add a dependency in case writer aborts
    if (writer != null && writer.isRunning()) {
      writer.readBy.add(this);
      this.readFrom.add(writer);
    }
    reads.put(key, version);
    tm.addReader(key, version, this);
  }

  public final void failedGet(Iterable<? extends K> keys, Throwable t) {
    checkRunning();
  }

  public final void beforePut(Iterable<? extends K> keys) throws IOException {
    for (K key : keys) {
      tm.writeLock(key);
      synchronized (this) {
        checkRunning();
        try {
          // enforce proper time-stamp ordering: abort transaction if needed
          NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = tm
              .getReaders(key);
          if (versions != null) {
            // reduce to versions that were written before this TA started
            for (NavigableSet<MVTOTransaction<K>> readers : versions.headMap(
                id.getTS(), false).values()) {
              // check if any version has been read by a TA that started after
              // this TA
              MVTOTransaction<K> reader = readers.higher(this);
              if (reader != null) {
                tm.writeUnlock(key);
                abort();
                throw new TransactionAbortedException("Transaction " + id
                    + " write conflict with " + reader.id);
              }
            }
          }

          /*
           * record write so we can clean it up if the TA aborts. If it is a
           * delete, we also use this information to filter deleted versions
           * from reads results and to delete values from the underlying data
           * store when the transaction commits
           */
          getTransactionLog().appendPut(id, key);
          /*
           * Add write in progress, so readers can check if they see the version
           */
          tm.addActiveWriter(key, this);
          addWrite(key);
        } finally {
          try {
            tm.writeUnlock(key);
          } catch (IllegalMonitorStateException e) {
            // ignore
          }
        }
      }
    }
  }

  public final void afterPut(Iterable<? extends K> keys) throws IOException {
    for (K key : keys) {
      tm.writeLock(key);
      try {
        synchronized (this) {
          checkRunning();
          tm.removeActiveWriter(key, this);
        }
      } finally {
        tm.writeUnlock(key);
      }
    }
  }

  public final void failedPut(Iterable<? extends K> keys, Throwable t)
      throws IOException {
    afterPut(keys);
  }

  // in-memory state transition
  protected void addWrite(K key) {
    this.lastTouched = System.currentTimeMillis();
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

  long getSID() {
    return this.sid;
  }

  synchronized int getState() {
    return state;
  }

  // blocks until read-from transactions complete
  synchronized void waitForReadFrom() throws IOException {
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
        throw new IOException("Commit interrupted");// IOException means: retry
      case BLOCKED:
        break;
      case COMMITTED:
      case FINALIZED:
        // 3. we should never get into one of these states
        throw newISA("doCommit");
      }
    }
  }

  synchronized void notifyReadBy() {
    for (MVTOTransaction<K> ta : readBy)
      if (readFrom.remove(ta) && readFrom.isEmpty())
        notify();
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
      tm.removeReader(key, read.getValue(), this);
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

}