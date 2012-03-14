package edu.illinois.htx.tm;

import static edu.illinois.htx.tm.Transaction.State.ABORTED;
import static edu.illinois.htx.tm.Transaction.State.ACTIVE;
import static edu.illinois.htx.tm.Transaction.State.BLOCKED;
import static edu.illinois.htx.tm.Transaction.State.COMMITTED;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.ipc.ProtocolSignature;

/**
 * Currently implements Multiversion Timestamp Ordering Protocol (MVTO) as
 * described in 'Transactional Information Systems' by Weikum and Vossen in
 * Section 5.5. The protocol consists of the following three rules:
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
 * for every write that does not have a matching read preceeding it in its
 * transaction.
 * 
 * TODO:
 * <ol>
 * <li>transaction time out</li>
 * <li>cleanup:
 * <ol>
 * <li>Versions in HBase that have been replaced with newer committed versions
 * can be deleted</li>
 * <li>Versions in HBase for committed deletes can be removed</li>
 * <li>Transactions for which all written versions have been removed can be
 * garbage collected</li>
 * <li>The head of the log can be truncated if it contains only log records for
 * garbage collected transactions</li>
 * <li>Versions in HBase for aborted transactions can be removed</li>
 * <li>Versions in HBase written by transactions that failed before notifying
 * the TM about the write can be removed (the exact timestamp is unknown, but it
 * eventually falls into a range that can be removed)</li>
 * </ol>
 * <li>reduce synchronization</li>
 * <li>it would be more efficient to also index transactions by Key</li>
 * <li>current assumption:
 * <ol>
 * <li>transactions execute a read, write, or delete only once per Key</li>
 * <li>transactions execute either a write or a delete for a Key, not both</li>
 * <li>transactions always execute a read for a Key before writing or deleting
 * it</li>
 * </ol>
 */
public class TransactionManager implements TransactionManagerInterface,
    VersionTracker {

  // recoverable components
  private final TimestampOracle tsOracle;
  private final TransactionLog tLog;

  // transient: would have to be reconstructed from the log after a crash
  private final NavigableMap<Long, Transaction> transactions;

  public TransactionManager() {
    this.tsOracle = new TimestampOracle();
    this.tLog = new TransactionLog();
    this.transactions = new TreeMap<Long, Transaction>();
  }

  @Override
  public long begin() {
    long tts = tsOracle.next();
    Transaction ta = new Transaction(tts);
    synchronized (this) {
      tLog.logBegin(tts);
      transactions.put(tts, ta);
    }
    return tts;
  }

  @Override
  public synchronized long selectReadVersion(long tts, Key key) {
    Transaction ta = getTransaction(tts);
    if (ta.getState() != ACTIVE)
      // TODO abort here?
      throw new IllegalStateException("Transaction " + tts + " not active");

    long timestamp = -1; // special value for not found
    Transaction writer = null;
    // 1. consider only transactions that started before this one
    for (Transaction wta : transactions.headMap(tts, false).descendingMap()
        .values()) {
      // do not read from aborted transactions
      if (wta.getState() == ABORTED)
        continue;
      // if value was deleted, treat like not found, but record read
      if (wta.hasDeleted(key)) {
        writer = wta;
        break;
      }
      // if value was written, remember timestamp and record read
      if (wta.hasWritten(key)) {
        writer = wta;
        timestamp = wta.getTransactionTimestamp();
        break;
      }
    }
    tLog.logRead(tts, key);
    if (writer != null) {
      ta.addReadFrom(writer);
      writer.addReadBy(ta);
    }
    /*
     * Note: it is possible that HBase contains a newer value the TM has not
     * received a notification about. This can happen if the reader was faster
     * than the writer (the HBase put and TM notification are not atomic) or the
     * writer has crashed, but the TM hasn't timed it out yet. However, I don't
     * think this poses a problem.
     */
    return timestamp;
  }

  @Override
  public synchronized void written(long tts, Key key)
      throws TransactionAbortedException {
    Transaction ta = getTransaction(tts);
    // TODO check state here
    tLog.logWrite(tts, key);
    ta.addWritten(key);
    checkConflict(tts, key);
  }

  @Override
  public synchronized void deleted(long tts, Key key)
      throws TransactionAbortedException {
    Transaction ta = getTransaction(tts);
    // TODO check state here
    tLog.logDelete(tts, key);
    ta.addDelete(key);
    checkConflict(tts, key);
  }

  // TODO it would probably be more efficient to add an index on transactions by
  // which keys they have read
  private void checkConflict(long tts, Key key)
      throws TransactionAbortedException {
    // 1. consider only transactions that started after this one
    for (Transaction ta : transactions.tailMap(tts, false).values()) {
      // 2. check if this transaction has read the value for the key
      Long ts = ta.getTimestampRead(key);
      // 3. if so, check if value was written before this transaction started
      if (ts != null && ts < tts)
        // TODO abort
        throw new TransactionAbortedException("Transaction " + tts
            + " write conflict with " + ts);
    }
  }

  @Override
  public void commit(long tts) throws TransactionAbortedException {
    synchronized (tLog) {
      // if still active
      Transaction ta = getTransaction(tts);
      switch (ta.getState()) {
      case ACTIVE:
        break;
      case BLOCKED:
        throw new IllegalStateException("Transaction " + tts
            + " commit pending");
      case ABORTED:
        throw new TransactionAbortedException("Transaction " + tts
            + " already aborted");
      case COMMITTED:
        return;
      }
      if (!ta.getReadFrom().isEmpty()) {
        ta.setState(BLOCKED);
        synchronized (ta) {
          while (!ta.getReadFrom().isEmpty()) {
            try {
              ta.wait();
            } catch (InterruptedException e) {
              Thread.interrupted();
            }
            if (ta.getState() == ABORTED)
              throw new TransactionAbortedException("Transaction " + tts
                  + " was aborted");
            if (ta.getState() == COMMITTED)
              return;
          }
        }
      }
      tLog.logCommit(tts);
      ta.setState(COMMITTED);
      for (Iterator<Transaction> it = ta.getReadBy().iterator(); it.hasNext();) {
        Transaction readBy = it.next();
        synchronized (readBy) {
          readBy.removeReadFrom(ta);
          if (readBy.getReadFrom().isEmpty())
            readBy.notifyAll();
        }
        it.remove();
      }
    }
  }

  @Override
  public synchronized void abort(long tts) {
    Transaction ta = getTransaction(tts);
    switch (ta.getState()) {
    case ACTIVE:
      break;
    case BLOCKED:
      synchronized (ta) {
        ta.setState(ABORTED);
        ta.notifyAll();
      }
      break;
    case ABORTED:
      return;
    case COMMITTED:
      throw new IllegalStateException("Transaction " + tts
          + " already committed");
    }
    tLog.logAbort(tts);
    ta.setState(ABORTED);
    for (Iterator<Transaction> it = ta.getReadBy().iterator(); it.hasNext();) {
      Transaction readBy = it.next();
      synchronized (readBy) {
        readBy.removeReadFrom(ta);
        abort(readBy.getTransactionTimestamp());
      }
      it.remove();
    }
    // TODO schedule cleanup of written/deleted rows
    ta.getWritten();
    ta.getDeleted();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return TransactionManagerInterface.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    long version = getProtocolVersion(protocol, clientVersion);
    return new ProtocolSignature(version, null);
  }

  synchronized Transaction getTransaction(long tts) {
    Transaction ta = transactions.get(tts);
    if (ta == null)
      throw new IllegalArgumentException("Transaction " + tts
          + " does not exist");
    return ta;
  }

}
