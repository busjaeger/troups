package edu.illinois.troups.tm.impl;

import static edu.illinois.troups.tm.XATransactionState.JOINED;
import static edu.illinois.troups.tm.XATransactionState.PREPARED;
import static edu.illinois.troups.tm.impl.TransientTransactionState.CREATED;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XATransactionLog;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class MVTOXATransaction<K extends Key> extends MVTOTransaction<K> {

  private static final Log LOG = LogFactory.getLog(MVTOXATransaction.class);

  public MVTOXATransaction(MVTOXATransactionManager<K, ?> tm) {
    super(tm);
  }

  @Override
  protected boolean shouldBegin() {
    throw newISA("Cannot begin XA transaction");
  }

  @Override
  protected boolean isRunning() {
    switch (state) {
    case JOINED:
    case PREPARED:
      return true;
    }
    return super.isRunning();
  }

  public synchronized void join(TID id) throws IOException {
    checkJoin();
    long ts = id.getTS();
    long pid = getTimestampManager().acquireReference(ts);
    XID xid = new XID(ts, pid);
    long sid = getTransactionLog().appendXAStateTransition(xid, JOINED);
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
    if (!shouldPrepare())
      return;
    waitForReadFrom();
    getTransactionLog().appendXAStateTransition(getID(), PREPARED);
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
      // check timestamp service here?
      return true;
    case JOINED:
      return true;
    default:
      return super.shouldAbort();
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
  public synchronized void timeout(long current, long timeout)
      throws IOException {
    if (state == PREPARED && (current - lastTouched) < timeout) {
      try {
        if (getTimestampManager().isReferencePersisted(getID().getTS(),
            getID().getPid())) {
          commit();
          return;
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Timeout commit failed " + this, e);
        return;
      }
    }
    super.timeout(current, timeout);
  }

  @Override
  protected SharedTimestampManager getTimestampManager() {
    return ((MVTOXATransactionManager<K, ?>) tm).getTimestampManager();
  }

  @Override
  protected XATransactionLog<K, ?> getTransactionLog() {
    return (XATransactionLog<K, ?>) tm.getTransactionLog();
  }

  synchronized XID getID() {
    return (XID) super.getID();
  }

}
