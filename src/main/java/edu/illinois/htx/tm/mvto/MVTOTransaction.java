package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ABORTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ACTIVE;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.COMMITTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.CREATED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.FINALIZED;

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
import edu.illinois.htx.tm.LogRecord.Type;
import edu.illinois.htx.tm.TransactionAbortedException;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  enum State {
    CREATED, ACTIVE, BLOCKED, ABORTED, COMMITTED, FINALIZED;
  }

  // immutable state
  private final MVTOTransactionManager<K, ?> tm;

  // mutable state
  private long id;
  private State state;
  private final Map<K, Long> reads;
  private final Map<K, Boolean> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final Set<MVTOTransaction<K>> readBy;

  public MVTOTransaction(MVTOTransactionManager<K, ?> tm) throws IOException {
    this.state = CREATED;
    this.tm = tm;
    this.reads = new HashMap<K, Long>();
    this.writes = new HashMap<K, Boolean>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
  }

  // ----------------------------------------------------------------------
  // state transition methods
  // ----------------------------------------------------------------------

  public synchronized void begin() throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ABORTED:
    case ACTIVE:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("begin");
    }
    doBegin();
  }

  // being == get/set ID
  protected void doBegin() throws IOException {
    setID(tm.getTimestampManager().next());
    tm.getLog().append(Type.BEGIN, id);
    setState(ACTIVE);
  }

  public synchronized void commit() throws TransactionAbortedException,
      IOException {
    switch (state) {
    case COMMITTED:
      return;
    case ACTIVE:
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

    tm.getLog().append(Type.COMMIT, id);
    setState(COMMITTED);
    tm.getTimestampManager().done(id);// TODO do this after finalize?

    /*
     * After a transaction commits, we still need to notify any waiting readers
     * and permanently remove deleted cells. We can do this on a separate
     * thread, since it is decoupled from the commit operation.
     */
    // TODO do async if no need to hold region lock (think through recovery)
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
     * permanently remove all rows deleted by this transaction
     */
    for (Iterator<Entry<K, Boolean>> it = writes.entrySet().iterator(); it
        .hasNext();) {
      Entry<K, Boolean> e = it.next();
      // remove deletes from data store so we don't have to keep this
      // transaction around to filter reads
      if (e.getValue())
        try {
          tm.getKeyValueStore().deleteVersions(e.getKey(), id);
        } catch (IOException e1) {
          // TODO retry
          e1.printStackTrace();
          return;
        }
      // writes were only kept around in case this TA aborts
      it.remove();
    }
    // logging once is sufficient, since delete operation idempotent
    tm.getLog().append(Type.FINALIZE, id);
    setState(FINALIZED);
  }

  public synchronized void abort() throws IOException {
    switch (state) {
    case ACTIVE:
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
    tm.getLog().append(Type.ABORT, id);
    if (state == BLOCKED)
      notify();
    setState(ABORTED);
    tm.getTimestampManager().done(id);

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
    // TODO handle system errors
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      MVTOTransaction<K> readBy = it.next();
      readBy.abort();
      it.remove();
    }

    // remove any writes in progress and cleanup data store
    for (Iterator<K> it = writes.keySet().iterator(); it.hasNext();) {
      K key = it.next();
      tm.removeWriter(key, this);
      try {
        tm.getKeyValueStore().deleteVersion(key, id);
      } catch (IOException e) {
        e.printStackTrace();
        // TODO handle internal errors (e.g. retry) to avoid resource leaks
        return;
      }
      it.remove();
    }

    tm.getLog().append(Type.FINALIZE, id);
    setState(FINALIZED);
  }

  public synchronized void afterRead(Iterable<? extends KeyVersions<K>> kvs)
      throws IOException {
    switch (state) {
    case ACTIVE:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("read");
    }

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
              case ACTIVE:
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
          NavigableSet<MVTOTransaction<K>> writes = tm.getWriters(key);
          if (writes != null) {
            MVTOTransaction<K> lastWrite = writes.lower(this);
            if (lastWrite != null && lastWrite.getID() > version) {
              abort();
              throw new TransactionAbortedException("Read conflict");
            }
          }

          // remember read for conflict detection
          tm.getLog().append(Type.READ, id, key, version);
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

  public synchronized void beforeWrite(boolean isDelete,
      Iterable<? extends K> keys) throws IOException {
    switch (state) {
    case ACTIVE:
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
        tm.getLog().append(isDelete ? Type.DELETE : Type.WRITE, id, key);
        addWrite(key, isDelete);
        /*
         * Add write in progress, so readers can check if they see the version
         */
        tm.addWriter(key, this);

      } finally {
        tm.unlock(key);
      }
    }
  }

  public synchronized void afterWrite(boolean isDelete,
      Iterable<? extends K> keys) {
    for (K key : keys) {
      tm.lock(key);
      try {
        /*
         * Remove write in progress, since we can be sure now it is present in
         * the data store
         */
        tm.removeWriter(key, this);
      } finally {
        tm.unlock(key);
      }
    }
  }

  // ----------------------------------------------------------------------
  // getter/setters for transaction state
  // used by state transitions methods and recovery process
  // ----------------------------------------------------------------------

  synchronized long getID() {
    if (state == State.CREATED)
      throw newISA("getID");
    return this.id;
  }

  synchronized void setID(long id) {
    this.id = id;
  }

  synchronized State getState() {
    return state;
  }

  // used by recovery
  synchronized void setState(State state) {
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
      case ACTIVE:
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
    readFrom.remove(ta);
    if (readFrom.isEmpty())
      notify();
  }

  synchronized void unblock() {
    if (state == BLOCKED) {
      setState(ACTIVE);
      notifyAll();// notify should be enough, but be safe
    }
  }

  synchronized void addReadBy(MVTOTransaction<K> transaction) {
    assert this.state != State.COMMITTED;
    readBy.add(transaction);
  }

  synchronized void removeReadBy() {
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      it.next().removeReadFrom(this);
      it.remove();
    }
  }

  synchronized void addRead(K key, long version) {
    reads.put(key, version);
  }

  synchronized void removeReads() {
    for (Iterator<Entry<K, Long>> it = reads.entrySet().iterator(); it
        .hasNext();) {
      Entry<K, Long> e = it.next();
      K key = e.getKey();
      tm.lock(key);
      try {
        tm.removeReader(key, e.getValue(), this);
      } finally {
        tm.unlock(key);
      }
      it.remove();
    }
  }

  synchronized void addWrite(K key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  private boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
  }

  private IllegalStateException newISA(String op) {
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