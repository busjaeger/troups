package edu.illinois.htx.tm.impl;

import static edu.illinois.htx.tm.TransactionState.ABORTED;
import static edu.illinois.htx.tm.TransactionState.COMMITTED;
import static edu.illinois.htx.tm.TransactionState.FINALIZED;
import static edu.illinois.htx.tm.TransactionState.STARTED;
import static edu.illinois.htx.tm.impl.LocalTransactionState.BLOCKED;
import static edu.illinois.htx.tm.impl.LocalTransactionState.CREATED;
import static edu.illinois.htx.tm.impl.XATransactionState.JOINED;
import static edu.illinois.htx.tm.impl.XATransactionState.PREPARED;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.XID;
import edu.illinois.htx.tm.log.XALog;
import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.SharedTimestampManager;
import edu.illinois.htx.tsm.TimestampManager.TimestampListener;

public class XAMVTOTransaction<K extends Key> extends MVTOTransaction<K>
    implements TimestampListener {

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

  public synchronized void join(TID id) throws IOException {
    checkJoin();
    long ts = id.getTS();
    long pid = getTimestampManager().acquireReference(ts);
    getTimestampManager().addTimestampListener(ts, this);
    long sid = getTransactionLog().appendXAStateTransition(getID(), JOINED);
    XID xid = new XID(ts, pid);
    setJoined(xid, sid);
  }

  protected void checkJoin() {
    switch (state) {
    case CREATED:
      break;
    default:
      throw newISA("join");
    }
  }

  protected void setJoined(XID id, long sid) {
    this.id = id;
    this.sid = sid;
    this.state = JOINED;
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    checkPrepare();
    waitForReadFrom();
    getTransactionLog().appendXAStateTransition(getID(), PREPARED);
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
      getTimestampManager().releaseReference(getID().getTS(), getID().getPid());
    } catch (NoSuchTimestampException e) {
      // ignore
    }
  }

  @Override
  protected SharedTimestampManager getTimestampManager() {
    return ((XAMVTOTransactionManager<K, ?>) tm).getTimestampManager();
  }

  @Override
  protected XALog<K, ?> getTransactionLog() {
    return (XALog<K, ?>) tm.getTransactionLog();
  }

  synchronized XID getID() {
    return (XID) super.getID();
  }

}
