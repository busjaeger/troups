package edu.illinois.troups.tm.impl;

import static edu.illinois.troups.tm.GroupTransactionState.ABORTED;
import static edu.illinois.troups.tm.GroupTransactionState.COMMITTED;
import static edu.illinois.troups.tm.GroupTransactionState.FINALIZED;
import static edu.illinois.troups.tm.GroupTransactionState.STARTED;
import static edu.illinois.troups.tm.impl.TransientTransactionState.BLOCKED;
import static edu.illinois.troups.tm.impl.TransientTransactionState.CREATED;
import static edu.illinois.troups.tm.CrossGroupTransactionState.JOINED;
import static edu.illinois.troups.tm.CrossGroupTransactionState.PREPARED;

import java.io.IOException;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.CrossGroupLog;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;
import edu.illinois.troups.tsm.TimestampManager.TimestampListener;

public class MVTOCrossGroupTransaction<K extends Key> extends MVTOGroupTransaction<K>
    implements TimestampListener {

  public MVTOCrossGroupTransaction(MVTOCrossGroupTransactionManager<K, ?> tm) {
    super(tm);
  }

  @Override
  protected boolean shouldBegin() {
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

  public synchronized void join(TID id, K groupKey) throws IOException {
    checkJoin();
    long ts = id.getTS();
    long pid = getTimestampManager().acquireReference(ts);
    XID xid = new XID(ts, pid);
    getTimestampManager().addTimestampListener(ts, this);
    long sid = getTransactionLog().appendCrossGroupStateTransition(xid,
        groupKey, JOINED);
    setJoined(xid, sid, groupKey);
  }

  protected void checkJoin() {
    switch (state) {
    case CREATED:
      break;
    default:
      throw newISA("join");
    }
  }

  protected void setJoined(XID id, long sid, K groupKey) {
    this.id = id;
    this.sid = sid;
    this.groupKey = groupKey;
    this.state = JOINED;
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    if (!shouldPrepare())
      return;
    waitForReadFrom();
    getTransactionLog().appendCrossGroupStateTransition(getID(), groupKey,
        PREPARED);
    setPrepared();
  }

  protected boolean shouldPrepare() {
    switch (state) {
    case JOINED:
      return true;
    case PREPARED:
      return false;
    default:
      throw newISA("checkPrepare");
    }
  }

  protected synchronized void setPrepared() {
    this.state = PREPARED;
  }

  @Override
  protected boolean shouldCommit() {
    switch (state) {
    case PREPARED:
      return true;
    case JOINED:
      return false;
    default:
      return super.shouldCommit();
    }
  }

  @Override
  protected boolean shouldAbort() {
    switch (state) {
    case PREPARED:
      return true;
    case JOINED:
      return true;
    default:
      return super.shouldAbort();
    }
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
    return ((MVTOCrossGroupTransactionManager<K, ?>) tm).getTimestampManager();
  }

  @Override
  protected CrossGroupLog<K, ?> getTransactionLog() {
    return (CrossGroupLog<K, ?>) tm.getTransactionLog();
  }

  synchronized XID getID() {
    return (XID) super.getID();
  }

}
