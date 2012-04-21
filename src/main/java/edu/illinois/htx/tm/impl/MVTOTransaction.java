package edu.illinois.htx.tm.impl;

import static edu.illinois.htx.tm.TransactionState.ABORTED;
import static edu.illinois.htx.tm.TransactionState.COMMITTED;
import static edu.illinois.htx.tm.TransactionState.FINALIZED;
import static edu.illinois.htx.tm.TransactionState.STARTED;
import static edu.illinois.htx.tm.impl.TransientTransactionState.BLOCKED;
import static edu.illinois.htx.tm.impl.TransientTransactionState.CREATED;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyVersions;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.log.Log;
import edu.illinois.htx.tsm.TimestampManager;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  // immutable state
  protected final MVTOTransactionManager<K, ?> tm;

  // mutable state
  protected TID id;
  protected long sid;
  protected int state;
  private final Map<K, Long> reads;
  private final Map<K, Boolean> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final Set<MVTOTransaction<K>> readBy;

  public MVTOTransaction(MVTOTransactionManager<K, ?> tm) {
    this.state = CREATED;
    this.tm = tm;
    this.reads = new HashMap<K, Long>();
    this.writes = new HashMap<K, Boolean>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
  }

  public final synchronized void begin() throws IOException {
    if (!shouldBegin())
      return;
    long ts = getTimestampManager().acquire();
    TID id = new TID(ts);
    long sid = getTransactionLog().appendStateTransition(id, STARTED);
    setStarted(id, sid);
  }

  protected boolean shouldBegin() {
    switch (state) {
    case CREATED:
      return true;
    default:
      throw newISA("begin");
    }
  }

  // in-memory state transition
  protected void setStarted(TID id, long sid) {
    this.id = id;
    this.sid = sid;
    this.state = STARTED;
  }

  public final synchronized void commit() throws TransactionAbortedException,
      IOException {
    if (!shouldCommit())
      return;
    waitForReadFrom();
    getTransactionLog().appendStateTransition(id, COMMITTED);
    setCommitted();
    finalizeCommit();
  }

  protected boolean shouldCommit() {
    switch (state) {
    case COMMITTED:
      return false;
    case STARTED:
      return true;
    default:
      throw newISA("commit");
    }
  }

  // in-memory state transition
  protected void setCommitted() {
    this.state = COMMITTED;
  }

  public final synchronized void abort() throws IOException {
    if (!shouldAbort())
      return;
    getTransactionLog().appendStateTransition(id, ABORTED);
    if (state == BLOCKED)
      notify();
    setAborted();
    finalizeAbort();
  }

  protected boolean shouldAbort() {
    switch (state) {
    case STARTED:
    case BLOCKED:
      return true;
    case ABORTED:
      return false;
    default:
      throw newISA("abort");
    }
  }

  // in-memory state transition
  protected void setAborted() {
    this.state = ABORTED;
    // This TA should no longer cause write conflicts, since it's aborted
    removeReads();
  }

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
  protected void finalizeCommit() throws IOException {
    /*
     * notify transactions that read from this one, that it committed (so they
     * don't wait on it): this has to be done after commit, since we don't want
     * to notify and then fail before commit.
     */
    // note: doing this up-front to unblock as quickly as possible
    notifyReadBy();

    /*
     * permanently remove all rows deleted by this transaction so we don't have
     * to continue to filter them after the TA is done
     */
    for (Entry<K, Boolean> write : writes.entrySet())
      if (write.getValue())
        tm.getKeyValueStore().deleteVersions(write.getKey(), id.getTS());

    // logging once is sufficient, since delete operation idempotent
    releaseTimestamp();
    getTransactionLog().appendStateTransition(id, FINALIZED);
    setCommitFinalized();
  }

  // in-memory state transition
  protected void setCommitFinalized() {
    notifyReadBy();
    state = FINALIZED;
  }

  protected void finalizeAbort() throws IOException {
    // cascade abort to transactions that read from this one
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      MVTOTransaction<K> readBy = it.next();
      readBy.abort();
      it.remove();
    }

    // remove any writes in progress and clean up data store
    for (Iterator<K> it = writes.keySet().iterator(); it.hasNext();) {
      K key = it.next();
      tm.lock(key);
      try {
        tm.removeActiveWriter(key, this);
      } finally {
        tm.unlock(key);
      }
      tm.getKeyValueStore().deleteVersion(key, id.getTS());
      it.remove();
    }

    releaseTimestamp();
    getTransactionLog().appendStateTransition(id, FINALIZED);
    setAbortFinalized();
  }

  // in-memory state transition
  protected void setAbortFinalized() {
    state = FINALIZED;
  }

  protected void releaseTimestamp() throws IOException {
    getTimestampManager().release(id.getTS());
  }

  public final synchronized void beforeGet(Iterable<? extends K> keys) {
    checkActive();
    tm.addActiveReader(this);
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
  public final synchronized void afterGet(Iterable<? extends KeyVersions<K>> kvs)
      throws IOException {
    checkActive();
    tm.removeActiveReader(this);
    for (KeyVersions<K> kv : kvs) {
      K key = kv.getKey();
      tm.lock(key);
      try {
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
              case CREATED:
                throw new IllegalStateException();
              case ABORTED:
                it.remove();
                continue;
              default:
                break;
              }
            }
            // if the version read is a delete, remove it from the result set
            if (writer.hasDeleted(key))
              it.remove();
          }

          /*
           * 2. Check if we have admitted a writer that started before this
           * transaction but whose version is not in the result set. If that's
           * the case, we cannot let this reader proceed, because reading an
           * older version would violate the serialization order (if this
           * transaction had already read the older value, the writer would have
           * never been admitted)
           */
          NavigableSet<MVTOTransaction<K>> writes = tm.getActiveWriters(key);
          if (writes != null) {
            MVTOTransaction<K> lastWrite = writes.lower(this);
            if (lastWrite != null
                && tm.getTimestampManager().compare(lastWrite.id.getTS(),
                    version) > 0) {
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
      } finally {
        tm.unlock(key);
      }
    }
  }

  // in-memory state transition
  protected void addGet(K key, long version) {
    MVTOTransaction<K> writer = tm.getTransaction(new TID(version));
    // if value is not yet committed, add a dependency in case writer aborts
    if (writer != null && writer.isActive()) {
      writer.readBy.add(this);
      this.readFrom.add(writer);
    }
    reads.put(key, version);
    tm.addReader(key, version, this);
  }

  public final synchronized void failedGet(Iterable<? extends K> keys,
      Throwable t) {
    checkActive();
    tm.removeActiveReader(this);
  }

  public final synchronized void beforePut(Iterable<? extends K> keys)
      throws IOException {
    beforeMutation(false, keys);
  }

  public final synchronized void afterPut(Iterable<? extends K> keys)
      throws IOException {
    afterMutation(false, keys);
  }

  public final synchronized void failedPut(Iterable<? extends K> keys,
      Throwable t) throws IOException {
    afterMutation(false, keys);
  }

  public final synchronized void beforeDelete(Iterable<? extends K> keys)
      throws IOException {
    beforeMutation(true, keys);
  }

  public final synchronized void afterDelete(Iterable<? extends K> keys)
      throws IOException {
    afterMutation(true, keys);
  }

  public final synchronized void failedDelete(Iterable<? extends K> keys,
      Throwable t) throws IOException {
    failedMutation(true, keys);
  }

  private void beforeMutation(boolean isDelete, Iterable<? extends K> keys)
      throws IOException {
    checkActive();

    for (K key : keys) {
      tm.lock(key);
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
              abort();
              throw new TransactionAbortedException("Transaction " + id
                  + " write conflict with " + reader.id);
            }
          }
        }

        /*
         * record write so we can clean it up if the TA aborts. If it is a
         * delete, we also use this information to filter deleted versions from
         * reads results and to delete values from the underlying data store
         * when the transaction commits
         */
        if (isDelete)
          getTransactionLog().appendDelete(id, key);
        else
          getTransactionLog().appendPut(id, key);
        /*
         * Add write in progress, so readers can check if they see the version
         */
        tm.addActiveWriter(key, this);
        addMutation(key, isDelete);
      } finally {
        tm.unlock(key);
      }
    }
  }

  // in-memory state transition
  protected void addMutation(K key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  private void afterMutation(boolean isDelete, Iterable<? extends K> keys) {
    checkActive();
    for (K key : keys) {
      tm.lock(key);
      try {
        tm.removeActiveWriter(key, this);
      } finally {
        tm.unlock(key);
      }
    }
  }

  private void failedMutation(boolean isDelete, Iterable<? extends K> keys) {
    checkActive();
    afterMutation(isDelete, keys);
  }

  // ----------------------------------------------------------------------
  // getter/setters for transaction state
  // used by state transitions methods and recovery process
  // ----------------------------------------------------------------------

  protected void checkActive() {
    if (!isActive())
      throw newISA("read");
  }

  protected boolean isActive() {
    return state == STARTED;
  }

  protected Log<K, ?> getTransactionLog() {
    return tm.getTransactionLog();
  }

  protected TimestampManager getTimestampManager() {
    return tm.getTimestampManager();
  }

  synchronized TID getID() {
    if (state == CREATED)
      throw newISA("getID");
    return this.id;
  }

  synchronized long getSID() {
    if (state == CREATED)
      throw newISA("getSID");
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
        // TODO add timeout
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
      case CREATED:
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
      tm.lock(key);
      try {
        tm.removeReader(key, read.getValue(), this);
      } finally {
        tm.unlock(key);
      }
    }
  }

  private boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
  }

  IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  @Override
  public int compareTo(MVTOTransaction<K> ta) {
    if (id == null)
      throw new IllegalStateException("no id");
    if (ta.id == null)
      throw new IllegalArgumentException("no ta id");
    return id.compareTo(ta.id);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (id == null)
      throw new IllegalStateException("no id");
    if (!(obj instanceof MVTOTransaction))
      return false;
    MVTOTransaction<K> ota = (MVTOTransaction<K>) obj;
    if (ota.id == null)
      throw new IllegalArgumentException("no ta id");
    return id.equals(ota.id);
  }

  @Override
  public int hashCode() {
    if (id == null)
      throw new IllegalStateException("no id");
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id + "(" + state + ")";
  }

}