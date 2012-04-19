package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.TransactionState.ABORTED;
import static edu.illinois.htx.tm.TransactionState.COMMITTED;
import static edu.illinois.htx.tm.TransactionState.FINALIZED;
import static edu.illinois.htx.tm.TransactionState.STARTED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.InternalTransactionState.BLOCKED;
import static edu.illinois.htx.tm.mvto.MVTOTransaction.InternalTransactionState.CREATED;
import static edu.illinois.htx.tm.mvto.XAMVTOTransaction.XATransactionState.JOINED;
import static edu.illinois.htx.tm.mvto.XAMVTOTransaction.XATransactionState.PREPARED;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.log.XALog;
import edu.illinois.htx.tsm.SharedTimestampManager;
import edu.illinois.htx.tsm.TimestampManager.TimestampListener;

public class XAMVTOTransaction<K extends Key> extends MVTOTransaction<K>
    implements TimestampListener {

  interface XATransactionState extends InternalTransactionState {
    public static final int JOINED = 6;
    public static final int PREPARED = 7;
  }

  private long pid;

  public XAMVTOTransaction(XAMVTOTransactionManager<K, ?> tm) {
    super(tm);
  }

  XAMVTOTransactionManager<K, ?> getTM() {
    return (XAMVTOTransactionManager<K, ?>) tm;
  }

  @Override
  protected void doBegin() throws IOException {
    throw newISA("Cannot begin XA transaction");
  }

  public synchronized void join(long id) throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ABORTED:
    case STARTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("join");
    }
    long pid = getTimestampManager().createReference(id);
    getTimestampManager().addTimestampListener(id, this);
    setID(id);
    setPID(pid);
    // if we fail here, TSM will clean up reference and client will abort
    setSID(getTransactionLog().appendJoinLogRecord(id, pid));
    setState(JOINED);
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    switch (state) {
    case STARTED:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("prepare");
    }
    waitUntilReadFromEmpty();
    getTransactionLog().appendStateTransition(id, PREPARED);
    setState(PREPARED);
  }

  /**
   * Transaction has been aborted: note this method will only be called if the
   * prepare phase was unsuccessful.
   * 
   * @param ts
   */
  @Override
  public synchronized void deleted(long ts) {
    switch (state) {
    case FINALIZED:
    case COMMITTED:
    case ABORTED:
      return;
    case STARTED:
    case BLOCKED:
      break;
    case CREATED:
      throw newISA("deleted");
    }
    try {
      abort();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void afterFinalize() throws IOException {
    getTimestampManager().releaseReference(id, pid);
  }

  @Override
  protected SharedTimestampManager getTimestampManager() {
    return ((XAMVTOTransactionManager<K, ?>) tm).getTimestampManager();
  }

  protected XALog<K, ?> getTransactionLog() {
    return ((XAMVTOTransactionManager<K, ?>) tm).getTransactionLog();
  }

  synchronized long getPID() {
    if (state == CREATED)
      throw newISA("getPID");
    return this.pid;
  }

  synchronized void setPID(long pid) {
    this.pid = pid;
  }

}
