package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ABORTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.ACTIVE;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.State.COMMITTED;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyVersion;
import edu.illinois.htx.tm.TransactionAbortedException;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long id;
  private State state;
  private boolean finalized;
  private final Map<K, Long> reads;
  private final Map<K, Boolean> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final Set<MVTOTransaction<K>> readBy;
  private final MVTOTransactionManager<K> tm;

  public MVTOTransaction(long id, MVTOTransactionManager<K> tm) {
    this.id = id;
    this.state = ACTIVE;
    this.reads = new HashMap<K, Long>();
    this.writes = new HashMap<K, Boolean>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
    this.tm = tm;
  }

  long getID() {
    return this.id;
  }

  public void begin() {
    tm.getLog().appendBegin(id);
    state = ACTIVE;
  }

  synchronized void commit() throws TransactionAbortedException {
    // check state
    switch (state) {
    case ACTIVE:
      break;
    case BLOCKED:
      throw new IllegalStateException("Transaction " + id + " commit pending");
    case ABORTED:
      throw new IllegalStateException("Transaction " + id + " already aborted");
    case COMMITTED:
      return;
    }

    // block until read-from transactions complete
    while (!readFrom.isEmpty()) {
      state = BLOCKED;
      try {
        // TODO re-think releasing lock here
        tm.getLock().unlock();
        // TODO add timeout
        wait();
        tm.getLock().lock();
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      /*
       * The following state changes can wake a blocked transaction: 1. the
       * read-from transaction aborted and cascaded its abort 2. the read-from
       * transaction committed 2. the transaction manager was shut down
       */
      if (state == ABORTED)
        throw new TransactionAbortedException("Transaction " + id
            + " is victim of cascading abort");
      if (!tm.isRunning())
        throw new IllegalStateException("Transaction Manager stopped");
    }

    // record commit
    tm.getLog().appendCommit(id);
    state = COMMITTED;

    /*
     * After a transaction commits, we still need to notify any waiting readers
     * and permanently remove deleted cells. We can do this on a separate
     * thread, since it is decoupled from the commit operation.
     */
    // for now we do it synchronously, so we still hold region lock (since we
    // assume no server crash ATM)
    cleanupAfterCommit();
  }

  synchronized void unblock() {
    if (state == BLOCKED) {
      state = ACTIVE;
      notify();
    }
  }

  synchronized void abort() {
    switch (state) {
    case ACTIVE:
    case BLOCKED:
      break;
    case ABORTED:
      return;
    case COMMITTED:
      throw new IllegalStateException("Transaction " + id
          + " already committed");
    }

    tm.getLog().appendAbort(id);
    if (state == BLOCKED)
      notify();
    state = ABORTED;

    // This TA should no longer cause write conflicts, since it's aborted
    removeReads();

    // abort any transactions that read from this one and undo writes
    cleanupAfterAbort();
  }

  // call with lock held
  private void cleanupAfterCommit() {
    /*
     * notify transactions that read from this one, that it committed (so they
     * don't wait on it): this has to be done after commit, since we don't want
     * to notify and then fail before commit.
     */
    for (Iterator<MVTOTransaction<K>> it = readBy.iterator(); it.hasNext();) {
      it.next().removeReadFrom(this);
      it.remove();
    }
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
    tm.getLog().appendFinalize(id);
    finalized = true;
  }

  private void cleanupAfterAbort() {
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

    tm.getLog().appendFinalize(id);
    finalized = true;
  }

  synchronized void setState(State state) {
    this.state = state;
  }

  synchronized State getState() {
    return state;
  }

  // TODO remove
  synchronized void setFinalized() {
    assert state == State.COMMITTED || state == State.ABORTED;
    this.finalized = true;
  }

  synchronized boolean isFinalized() {
    return finalized;
  }

  synchronized void addReadFrom(MVTOTransaction<K> ta) {
    assert ta.state != COMMITTED;
    readFrom.add(ta);
  }

  synchronized void removeReadFrom(MVTOTransaction<K> ta) {
    assert ta.state == COMMITTED;
    readFrom.remove(ta);
    if (readFrom.isEmpty())
      notify();
  }

  // TODO lock/unlock should be in finally blocks
  synchronized void afterRead(Iterable<? extends KeyVersion<K>> versions)
      throws TransactionAbortedException {
    // check state
    switch (state) {
    case ACTIVE:
      break;
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
      throw new IllegalStateException("Transaction in state " + state);
    }

    Iterator<? extends KeyVersion<K>> it = versions.iterator();
    if (it.hasNext()) {
      KeyVersion<K> kv = it.next();
      tm.lock(kv.getKey());
      outer: while (true) {
        K key = kv.getKey();
        long version = kv.getVersion();

        MVTOTransaction<K> writer = tm.getTransaction(version);
        // if we don't have a TA, assume TA is committed and GC'ed
        if (writer != null) {
          synchronized (writer) {
            switch (writer.state) {
            case ACTIVE:
            case BLOCKED:
              // value not yet committed, add dependency in case writer aborts
              writer.readBy.add(this);
              this.readFrom.add(writer);
              // value deleted
              if (writer.hasDeleted(key))
                it.remove();
              break;
            case ABORTED:
              // value written by aborted TA not yet GC'ed, remove from results
              it.remove();
              continue;
            case COMMITTED:
              // value deleted, but not yet cleaned up
              if (writer.hasDeleted(key))
                it.remove();
              break;
            }
          }
        }

        /*
         * Check if we have admitted a writer that started before this
         * transaction but whose version is not in the result set. If that's the
         * case, we cannot let this reader proceed, because reading an older
         * version would violate the serialization order (if this transaction
         * had already read the older value, the writer would have never been
         * admitted)
         */
        NavigableSet<MVTOTransaction<K>> writes = tm.getWriters(key);
        if (writes != null) {
          MVTOTransaction<K> lastWrite = writes.lower(this);
          if (lastWrite != null && lastWrite.getID() > version) {
            tm.unlock(key);
            abort();
            throw new TransactionAbortedException("Read conflict");
          }
        }

        // remember read for conflict detection
        tm.getLog().appendRead(id, key, version);
        addRead(key, version);
        tm.addReader(key, version, this);

        // older versions not yet cleaned up, remove from results
        while (it.hasNext()) {
          KeyVersion<K> next = it.next();
          if (kv.getVersion() != next.getVersion()) {
            tm.unlock(key);
            kv = next;
            tm.lock(kv.getKey());
            continue outer;
          }
          it.remove();
        }
        tm.unlock(key);
        // no further elements
        break;
      }
    }
  }

  synchronized void beforeWrite(boolean isDelete, Iterable<? extends K> keys)
      throws TransactionAbortedException {
    switch (state) {
    case ACTIVE:
      break;
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
      throw new IllegalStateException("Transaction in state " + state);
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
         * Add write in progress, so readers can check if they see the version
         */
        tm.addWriter(key, this);

        /*
         * record write so we can clean it up if the TA aborts. If it is a
         * delete, we also use this information to filter deleted versions from
         * reads results and to delete values from the underlying data store
         * when the transaction commits
         */
        tm.getLog().appendWrite(id, key, isDelete);
        addWrite(key, isDelete);
      } finally {
        tm.unlock(key);
      }
    }
  }

  synchronized void afterWrite(boolean isDelete, Iterable<? extends K> keys) {
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

  synchronized void addReadBy(MVTOTransaction<K> transaction) {
    assert this.state != State.COMMITTED;
    readBy.add(transaction);
  }

  Collection<MVTOTransaction<K>> getReadBy() {
    return readBy;
  }

  synchronized void addRead(K key, long version) {
    reads.put(key, version);
  }

  synchronized void removeReads() {
    for (Iterator<Entry<K, Long>> it = reads.entrySet().iterator(); it
        .hasNext();) {
      Entry<K, Long> e = it.next();
      tm.removeReader(e.getKey(), e.getValue(), this);
      it.remove();
    }
  }

  synchronized void addWrite(K key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  private boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
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
    return id + ":" + state + (finalized ? "(finalized)" : "");
  }

}