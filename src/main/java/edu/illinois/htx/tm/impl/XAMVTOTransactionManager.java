package edu.illinois.htx.tm.impl;

import static edu.illinois.htx.tm.XATransactionState.JOINED;
import static edu.illinois.htx.tm.XATransactionState.PREPARED;
import static edu.illinois.htx.tm.log.Log.RECORD_TYPE_STATE_TRANSITION;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.XATransactionManager;
import edu.illinois.htx.tm.XID;
import edu.illinois.htx.tm.log.LogRecord;
import edu.illinois.htx.tm.log.StateTransitionLogRecord;
import edu.illinois.htx.tm.log.XALog;
import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.SharedTimestampManager;

public class XAMVTOTransactionManager<K extends Key, R extends LogRecord>
    extends MVTOTransactionManager<K, R> implements XATransactionManager {

  public XAMVTOTransactionManager(KeyValueStore<K> keyValueStore,
      XALog<K, R> log, SharedTimestampManager timestampManager) {
    super(keyValueStore, log, timestampManager);
  }

  @Override
  public SharedTimestampManager getTimestampManager() {
    return (SharedTimestampManager) timestampManager;
  }

  @Override
  public XALog<K, R> getTransactionLog() {
    return (XALog<K, R>) transactionLog;
  }

  @Override
  public synchronized XID join(final TID tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      XAMVTOTransaction<K> ta = new XAMVTOTransaction<K>(this);
      ta.join(tid);
      addTransaction(ta);
      return ta.getID();
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void prepare(final XID xid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        if (!(ta instanceof XAMVTOTransaction))
          throw new IllegalStateException("Cannot prepare non XA transaction");
        ((XAMVTOTransaction<K>) ta).prepare();
        System.out.println("Prepared " + xid);
      }
    }.run(xid);
  }

  @Override
  public void commit(XID xid, boolean onePhase) throws IOException {
    if (onePhase) {
      new WithReadLock() {
        @Override
        void execute(MVTOTransaction<K> ta) throws IOException {
          XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
          xta.setPrepared();
          xta.commit();
        }
      }.run(xid);
    } else {
      super.commit(xid);
    }
  }

  @Override
  public void abort(XID xid) throws IOException {
    super.abort(xid);
  }

  @Override
  protected void replay(LogRecord record) {
    TID tid = record.getTID();
    MVTOTransaction<K> ta = getTransaction(tid);
    switch (record.getType()) {
    case RECORD_TYPE_STATE_TRANSITION:
      StateTransitionLogRecord stlr = (StateTransitionLogRecord) record;
      switch (stlr.getTransactionState()) {
      case PREPARED: {
        if (ta == null)
          return;
        if (!(ta instanceof XAMVTOTransaction))
          throw new IllegalStateException(
              "prepare log record for non-XA transaction");
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        xta.setPrepared();
        return;
      }
      case JOINED: {
        if (ta != null)
          throw new IllegalStateException(
              "join record for existing transaction");
        XAMVTOTransaction<K> xta = new XAMVTOTransaction<K>(this);
        long sid = stlr.getSID();
        xta.setJoined((XID) record.getTID(), sid);
        addTransaction(ta);
        return;
      }
      }
      break;
    }
    super.replay(record);
  }

  @Override
  protected void recover(MVTOTransaction<K> ta) {
    try {
      switch (ta.getState()) {
      case JOINED: {
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        XID xid = xta.getID();
        try {
          // TODO check if any operations failed in the middle
          // still a chance to complete this transaction, could also just abort
          if (!getTimestampManager().isReleased(xid.getTS())
              && getTimestampManager().isReferenceHeldByMe(xid.getTS(),
                  xid.getPid()))
            return;
        } catch (NoSuchTimestampException e) {
          // fall through
        }
        xta.abort();
        return;
      }
      case PREPARED: {
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        XID xid = xta.getID();
        // first try to figure out if we need to commit
        try {
          if (getTimestampManager().isReferencePersisted(xid.getTS(),
              xid.getPid())) {
            xta.commit();
            return;
          }
        } catch (NoSuchTimestampException e) {
          // fall through
        }
        // TODO check if any operations failed in the middle
        // if not, check if the transaction is still active, could also abort
        try {
          if (!getTimestampManager().isReleased(xid.getTS())
              && getTimestampManager().isReferenceHeldByMe(xid.getTS(),
                  xid.getPid()))
            return;
        } catch (NoSuchTimestampException e) {
          // fall through
        }
        // otherwise safe and probably best to abort
        xta.abort();
        return;
      }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    super.recover(ta);
  }

  @Override
  protected void reclaim(MVTOTransaction<K> ta) {
    switch (ta.getState()) {
    case JOINED:
    case PREPARED:
      // we could double-check with TSM that prepare really is aborted
      try {
        ta.abort();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return;
    }
    super.reclaim(ta);
  }

}
