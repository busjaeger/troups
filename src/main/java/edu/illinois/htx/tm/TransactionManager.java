package edu.illinois.htx.tm;

import static edu.illinois.htx.tm.Transaction.State.ABORTED;
import static edu.illinois.htx.tm.Transaction.State.BLOCKED;
import static edu.illinois.htx.tm.Transaction.State.COMMITTED;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;

import edu.illinois.htx.tm.Transaction.State;

/**
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
 * <li>delete unused versions (needs call-back interface into HBase):
 * <ol>
 * <li>Versions that are superseded by newer committed version.
 * <li>Versions written by committed transactions that are deletes.
 * <li>Versions written by aborted transactions.
 * </ol>
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
public class TransactionManager implements TransactionManagerProtocol {

  /**
   * TODO
   * <p>
   * recover these data structures after a server crash
   * <p>
   * clean up these data structures when they are no longer needed
   */

  // transactions by transaction ID for efficient direct lookup
  private final Map<Long, Transaction> transactions;
  // transactions indexed by read versions for efficient conflict detection
  private final Map<Key, NavigableMap<Long, NavigableSet<Transaction>>> transactionsByVersionsRead;

  public TransactionManager() {
    transactions = new HashMap<Long, Transaction>();
    transactionsByVersionsRead = new TreeMap<Key, NavigableMap<Long, NavigableSet<Transaction>>>();
  }

  /**
   * Get the transaction object for the given timestamp
   * 
   * @param tid
   * @return
   */
  private Transaction getTransaction(long tid) {
    Transaction ta = transactions.get(tid);
    if (ta == null)
      throw new IllegalStateException("Transaction " + tid + "does not exist");
    return ta;
  }

  /**
   * Indexes the given transaction by the key version it read.
   * 
   * @param key
   * @param version
   * @param ta
   */
  private void index(Key key, long version, Transaction ta) {
    NavigableMap<Long, NavigableSet<Transaction>> versions = transactionsByVersionsRead
        .get(key);
    if (versions == null)
      transactionsByVersionsRead.put(key,
          versions = new TreeMap<Long, NavigableSet<Transaction>>());
    NavigableSet<Transaction> readers = versions.get(version);
    if (readers == null)
      versions.put(version, readers = new TreeSet<Transaction>());
    readers.add(ta);
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
  public synchronized void filterReads(long tid, List<? extends KeyValue> kvs) {
    Transaction reader = getTransaction(tid);
    if (reader.getState() != State.ACTIVE)
      throw new IllegalStateException(
          "Cannot perform read for inactive transaction");

    ListIterator<? extends KeyValue> it = kvs.listIterator();
    while (it.hasNext()) {
      KeyValue kv = it.next();
      Key key = new Key(kv);
      long version = kv.getTimestamp();
      Transaction writer = transactions.get(version);

      // TODO consider this case once recovery is thought through
      if (writer == null)
        throw new IllegalStateException("no transaction for version " + version);

      switch (writer.getState()) {
      case ACTIVE:
      case BLOCKED:
        // value not yet committed, add dependency in case writer aborts
        writer.addReadBy(reader);
        reader.addReadFrom(writer);
      case ABORTED:
        // value written by aborted TA & not yet cleaned up, remove from results
        it.remove();
        continue;
      case COMMITTED:
        break;
      }

      // TODO log read

      // remember read so we can check write conflicts later
      index(key, version, reader);

      // value deleted by running or committed TA, remove from results
      if (writer.hasDeleted(key))
        it.remove();

      // older versions not yet cleaned up, remove from results
      while (it.hasNext()) {
        KeyValue next = it.next();
        if (next.getTimestamp() != version) {
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
  public synchronized void write(long tid, KeyValue kv, boolean isDelete)
      throws TransactionAbortedException {
    Transaction writer = getTransaction(tid);
    if (writer.getState() != State.ACTIVE)
      throw new IllegalStateException(
          "Cannot perform write for inactive transaction");
    if (tid != kv.getTimestamp())
      throw new IllegalArgumentException(
          "KeyValue time-stamp does not match transaction timestamp");

    Key key = new Key(kv);

    // TODO log write for recovery

    // 1. record the mutation so we can clean it up if the TA aborts later
    // and so we can filter out deleted cells for reads.
    writer.addWrite(key, isDelete);

    // 2. enforce proper time-stamp ordering: abort transaction if needed
    NavigableMap<Long, NavigableSet<Transaction>> versions = transactionsByVersionsRead
        .get(key);
    if (versions != null) {
      // reduce to versions that were written before this TA started
      for (NavigableSet<Transaction> readers : versions.headMap(tid, false)
          .values()) {
        // check if any version has been read by a TA that started after this TA
        Transaction reader = readers.higher(writer);
        if (reader != null) {
          abort(reader.getID());
          throw new TransactionAbortedException("Transaction " + tid
              + " cannot write, because " + reader
              + " which started after it has already read an older version.");
        }
      }
    }
  }

  @Override
  public void begin(long tid) {
    Transaction ta = transactions.get(tid);
    if (ta != null)
      throw new IllegalStateException("Transaction " + tid + " already exists");
    transactions.put(tid, ta);
  }

  @Override
  public synchronized void commit(long tid) throws TransactionAbortedException {
    // only continue if transaction is active
    Transaction ta = getTransaction(tid);
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
    for (Iterator<Transaction> it = ta.getReadBy().iterator(); it.hasNext();) {
      Transaction readBy = it.next();
      synchronized (readBy) {
        readBy.removeReadFrom(ta);
        if (readBy.getReadFrom().isEmpty())
          readBy.notify();
      }
      it.remove();
    }

    // TODO schedule cleanup of deleted rows - or handle as part of GC

    System.out.println("Committed " + tid);
  }

  @Override
  public synchronized void abort(long tid) {
    Transaction ta = getTransaction(tid);
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
    for (Iterator<Transaction> it = ta.getReadBy().iterator(); it.hasNext();) {
      Transaction readBy = it.next();
      synchronized (readBy) {
        readBy.removeReadFrom(ta);
        abort(readBy.getID());
      }
      it.remove();
    }

    // TODO schedule clean up of all written rows - or handle as part of GC
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return TransactionManagerProtocol.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }

}
