package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.TransactionState.ABORTED;
import static edu.illinois.htx.tm.TransactionState.COMMITTED;
import static edu.illinois.htx.tm.TransactionState.FINALIZED;
import static edu.illinois.htx.tm.TransactionState.STARTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.InternalTransactionState.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.InternalTransactionState.CREATED;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyVersions;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.TransactionState;
import edu.illinois.htx.tm.log.Log;
import edu.illinois.htx.tsm.TimestampManager;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  interface InternalTransactionState extends TransactionState {
    public static final int CREATED = 5;
    public static final int BLOCKED = 6;
  }

  // immutable state
  protected final MVTOTransactionManager<K, ?> tm;

  // mutable state
  protected long id;
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
    checkBegin();
    doBegin();
  }

  protected void checkBegin() {
    switch (state) {
    case CREATED:
      break;
    default:
      throw newISA("begin");
    }
  }

  // being == get/set ID
  protected void doBegin() throws IOException {
    setID(getTimestampManager().create());
    setSID(getTransactionLog().appendStateTransition(id, STARTED));
    setState(STARTED);
  }

  public final synchronized void commit() throws TransactionAbortedException,
      IOException {
    switch (state) {
    case COMMITTED:
      return;
    case STARTED:
      break;
    case CREATED:
    case BLOCKED:
    case ABORTED:
    case FINALIZED:
      throw newISA("commit");
    }
    doCommit();
  }

  protected void doCommit() throws TransactionAbortedException, IOException {
    waitUntilReadFromEmpty();

    getTransactionLog().appendStateTransition(id, COMMITTED);
    setState(COMMITTED);

    /*
     * After a transaction commits, we still need to notify any waiting readers
     * and permanently remove deleted cells. We can do this on a separate
     * thread, since it is decoupled from the commit operation.
     */
    afterCommit();
  }

  // call with lock held
  protected void afterCommit() throws IOException {
    /*
     * notify transactions that read from this one, that it committed (so they
     * don't wait on it): this has to be done after commit, since we don't want
     * to notify and then fail before commit.
     */
    removeReadBy();

    /*
     * permanently remove all rows deleted by this transaction so we don't have
     * to continue to filter them after the TA is done
     */
    for (Entry<K, Boolean> write : writes.entrySet())
      if (write.getValue())
        try {
          tm.getKeyValueStore().deleteVersions(write.getKey(), id);
        } catch (IOException e) {
          // could retry a couple times
          e.printStackTrace();
          return;
        }

    // logging once is sufficient, since delete operation idempotent
    getTransactionLog().appendStateTransition(id, FINALIZED);
    setState(FINALIZED);
    afterFinalize();
  }

  public final synchronized void abort() throws IOException {
    switch (state) {
    case STARTED:
    case BLOCKED:
      break;
    case ABORTED:
      return;
    case CREATED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("abort");
    }
    doAbort();
  }

  protected void doAbort() throws IOException {
    getTransactionLog().appendStateTransition(id, ABORTED);
    if (state == BLOCKED)
      notify();
    setState(ABORTED);

    // This TA should no longer cause write conflicts, since it's aborted
    removeReads();

    // abort any transactions that read from this one and undo writes
    afterAbort();
  }

  protected void afterAbort() throws IOException {
    // we can forget who we read from: will never commit and therefore
    // never need to wait on the TAs we read from to complete
    readFrom.clear();

    // cascade abort to transactions that read from this one
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      MVTOTransaction<K> readBy = it.next();
      readBy.abort();
      it.remove();
    }

    // remove any writes in progress and cleanup data store
    for (Iterator<K> it = writes.keySet().iterator(); it.hasNext();) {
      K key = it.next();
      tm.removeActiveWriter(key, this);
      try {
        tm.getKeyValueStore().deleteVersion(key, id);
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }
      it.remove();
    }

    getTransactionLog().appendStateTransition(id, FINALIZED);
    setState(FINALIZED);
    afterFinalize();
  }

  protected void afterFinalize() throws IOException {
    getTimestampManager().delete(id);
  }

  public final synchronized void beforeGet(Iterable<? extends K> keys) {
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }
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
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }

    tm.removeActiveReader(this);
    for (KeyVersions<K> kv : kvs) {
      K key = kv.getKey();
      tm.lock(key);
      try {
        for (Iterator<Long> it = kv.getVersions().iterator(); it.hasNext();) {
          long version = it.next();
          MVTOTransaction<K> writer = tm.getTransaction(version);
          // if we don't have a TA, assume TA is committed and GC'ed
          if (writer != null) {
            synchronized (writer) {
              switch (writer.state) {
              case CREATED:
                throw new IllegalStateException();
              case STARTED:
              case BLOCKED:
                // value not yet committed, add dependency in case writer aborts
                writer.addReadBy(this);
                addReadFrom(writer);
                // value deleted
                if (writer.hasDeleted(key))
                  it.remove();
                break;
              case ABORTED:
                // value written by aborted TA not yet GC'ed, remove from
                // results
                it.remove();
                continue;
              case COMMITTED:
              case FINALIZED:
                // value deleted, but not yet cleaned up
                if (writer.hasDeleted(key))
                  it.remove();
                break;
              }
            }
          }

          /*
           * Check if we have admitted a writer that started before this
           * transaction but whose version is not in the result set. If that's
           * the case, we cannot let this reader proceed, because reading an
           * older version would violate the serialization order (if this
           * transaction had already read the older value, the writer would have
           * never been admitted)
           */
          NavigableSet<MVTOTransaction<K>> writes = tm.getActiveWriters(key);
          if (writes != null) {
            MVTOTransaction<K> lastWrite = writes.lower(this);
            if (lastWrite != null && lastWrite.getID() > version) {
              abort();
              throw new TransactionAbortedException("Read conflict");
            }
          }

          // remember read for conflict detection
          getTransactionLog().appendGet(id, key, version);
          addRead(key, version);
          tm.addReader(key, version, this);

          // remove all older versions from result set
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

  public final synchronized void failedGet(Iterable<? extends K> keys,
      Throwable t) {
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }
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
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }

    for (K key : keys) {
      tm.lock(key);
      try {
        // enforce proper time-stamp ordering: abort transaction if needed
        NavigableMap<Long, NavigableSet<MVTOTransaction<K>>> versions = tm
            .getReaders(key);
        if (versions != null) {
          // reduce to versions that were written before this TA started
          for (NavigableSet<MVTOTransaction<K>> readers : versions.headMap(id,
              false).values()) {
            // check if any version has been read by a TA that started after
            // this TA
            MVTOTransaction<K> reader = readers.higher(this);
            if (reader != null) {
              abort();
              throw new TransactionAbortedException("Transaction " + id
                  + " write conflict with " + reader.getID());
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

        addWrite(key, isDelete);
        /*
         * Add write in progress, so readers can check if they see the version
         */
        tm.addActiveWriter(key, this);

      } finally {
        tm.unlock(key);
      }
    }
  }

  private void afterMutation(boolean isDelete, Iterable<? extends K> keys) {
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }

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
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }

    afterMutation(isDelete, keys);
  }

  // ----------------------------------------------------------------------
  // getter/setters for transaction state
  // used by state transitions methods and recovery process
  // ----------------------------------------------------------------------

  protected Log<K, ?> getTransactionLog() {
    return tm.getTransactionLog();
  }

  protected TimestampManager getTimestampManager() {
    return tm.getTimestampManager();
  }

  synchronized long getID() {
    if (state == CREATED)
      throw newISA("getID");
    return this.id;
  }

  synchronized void setID(long id) {
    this.id = id;
  }

  synchronized long getSID() {
    if (state == CREATED)
      throw newISA("getSID");
    return this.sid;
  }

  synchronized void setSID(long sid) {
    this.sid = sid;
  }

  synchronized int getState() {
    return state;
  }

  // used by recovery
  synchronized void setState(int state) {
    this.state = state;
  }

  // used by recovery
  synchronized void addReadFrom(MVTOTransaction<K> ta) {
    assert ta.state != COMMITTED;
    readFrom.add(ta);
  }

  // blocks until read-from transactions complete
  synchronized void waitUntilReadFromEmpty() throws IOException {
    while (!readFrom.isEmpty()) {
      setState(BLOCKED);
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

  synchronized void removeReadFrom(MVTOTransaction<K> ta) {
    assert ta.state == COMMITTED;
    if (readFrom.remove(ta) && readFrom.isEmpty())
      notify();
  }

  synchronized void unblock() {
    if (state == BLOCKED) {
      setState(STARTED);
      notifyAll();// notify should be enough, but be safe
    }
  }

  synchronized void addReadBy(MVTOTransaction<K> transaction) {
    assert this.state != COMMITTED;
    readBy.add(transaction);
  }

  synchronized void removeReadBy() {
    for (MVTOTransaction<K> ta : readBy)
      ta.removeReadFrom(this);
  }

  synchronized void addRead(K key, long version) {
    reads.put(key, version);
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

  synchronized void addWrite(K key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  private boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
  }

  IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  @Override
  public int compareTo(MVTOTransaction<K> ta) {
    return Long.valueOf(id).compareTo(ta.id);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (!(obj instanceof MVTOTransaction))
      return false;
    return id == ((MVTOTransaction<K>) obj).id;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(id).hashCode();
  }

  @Override
  public String toString() {
    return id + "(" + state + ")";
  }

}