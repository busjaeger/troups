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
import edu.illinois.htx.tsm.NoSuchTimestampException;
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

  @Override
  protected void checkBegin() {
    throw newISA("Cannot begin XA transaction");
  }

  @Override
  protected boolean isActive() {
    switch (state) {
    case JOINED:
    case PREPARED:
      return true;
    }
    return super.isActive();
  }

  public synchronized void join(long id) throws IOException {
    checkJoin();
    long pid = getTimestampManager().acquireReference(id);
    getTimestampManager().addTimestampListener(id, this);
    long sid = getTransactionLog().appendJoinLogRecord(id, pid);
    setJoined(id, pid, sid);
  }

  protected void checkJoin() {
    switch (state) {
    case CREATED:
      break;
    default:
      throw newISA("join");
    }
  }

  protected void setJoined(long id, long pid, long sid) {
    this.id = id;
    this.pid = pid;
    this.sid = sid;
    this.state = JOINED;
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    checkPrepare();
    waitForReadFrom();
    getTransactionLog().appendStateTransition(id, PREPARED);
    setPrepared();
  }

  protected void checkPrepare() {
    switch (state) {
    case JOINED:
      break;
    default:
      throw newISA("checkPrepare");
    }
  }

  protected void setPrepared() {
    this.sid = PREPARED;
  }

  /**
   * Transaction has been aborted: note this method will only be called if the
   * prepare phase was unsuccessful.
   * 
   * @param ts
   */
  @Override
  public synchronized void released(long ts) {
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
  protected void releaseTimestamp() throws IOException {
    try {
      getTimestampManager().releaseReference(id, pid);
    } catch (NoSuchTimestampException e) {
      // ignore
    }
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

}
