package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.XATransactionState.JOINED;
import static edu.illinois.troups.tm.XATransactionState.PREPARED;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.XATransactionLog;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class MVTOXATransaction<K extends Key> extends MVTOTransaction<K> {

  private static final Log LOG = LogFactory.getLog(MVTOXATransaction.class);

  public MVTOXATransaction(MVTOXATransactionManager<K, ?> tm, XID tid, long sid,
      int state) {
    super(tm, tid, sid, state);
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
  public synchronized void timeout(long timeout) throws IOException {
    long current = System.currentTimeMillis();
    if (state == PREPARED && (current - lastTouched) < timeout) {
      try {
        if (getTimestampManager().isReferencePersisted(getID().getTS(),
            getID().getPid())) {
          LOG.info("Timeout committing prepared transaction");
          commit();
          return;
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Timeout commit failed " + this, e);
        e.printStackTrace(System.out);
        return;
      }
    }
    super.timeout(timeout);
  }

  @Override
  protected SharedTimestampManager getTimestampManager() {
    return ((MVTOXATransactionManager<K, ?>) tm).getTimestampManager();
  }

  @Override
  protected XATransactionLog<K, ?> getTransactionLog() {
    return (XATransactionLog<K, ?>) tm.getTransactionLog();
  }

  XID getID() {
    return (XID) super.getID();
  }

}
