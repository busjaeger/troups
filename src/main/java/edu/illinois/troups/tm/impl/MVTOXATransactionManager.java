package edu.illinois.troups.tm.impl;

import static edu.illinois.troups.tm.XATransactionLog.RECORD_TYPE_XA_STATE_TRANSITION;
import static edu.illinois.troups.tm.XATransactionState.JOINED;
import static edu.illinois.troups.tm.XATransactionState.PREPARED;

import java.io.IOException;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog.Record;
import edu.illinois.troups.tm.XATransactionLog.XAStateTransitionRecord;
import edu.illinois.troups.tm.XATransactionLog;
import edu.illinois.troups.tm.XATransactionManager;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class MVTOXATransactionManager<K extends Key, R extends Record<K>>
    extends MVTOTransactionManager<K, R> implements XATransactionManager {

  public MVTOXATransactionManager(KeyValueStore<K> keyValueStore,
      XATransactionLog<K, R> log, SharedTimestampManager timestampManager) {
    super(keyValueStore, log, timestampManager);
  }

  @Override
  public SharedTimestampManager getTimestampManager() {
    return (SharedTimestampManager) timestampManager;
  }

  @Override
  public XATransactionLog<K, R> getTransactionLog() {
    return (XATransactionLog<K, R>) transactionLog;
  }

  @Override
  public synchronized XID join(final TID tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      MVTOXATransaction<K> ta = new MVTOXATransaction<K>(this);
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
        if (!(ta instanceof MVTOXATransaction))
          throw new IllegalStateException("Cannot prepare non XA transaction");
        ((MVTOXATransaction<K>) ta).prepare();
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
          MVTOXATransaction<K> xta = (MVTOXATransaction<K>) ta;
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
  protected void replay(long sid, R record) {
    TID tid = record.getTID();
    MVTOTransaction<K> ta = getTransaction(tid);
    switch (record.getType()) {
    case RECORD_TYPE_XA_STATE_TRANSITION:
      XAStateTransitionRecord<K> stlr = (XAStateTransitionRecord<K>) record;
      switch (stlr.getTransactionState()) {
      case PREPARED: {
        if (ta == null)
          return;
        if (!(ta instanceof MVTOXATransaction))
          throw new IllegalStateException(
              "prepare log record for non-XA transaction");
        MVTOXATransaction<K> xta = (MVTOXATransaction<K>) ta;
        xta.setPrepared();
        return;
      }
      case JOINED: {
        if (ta != null)
          throw new IllegalStateException(
              "join record for existing transaction");
        MVTOXATransaction<K> xta = new MVTOXATransaction<K>(
            this);
        xta.setJoined((XID) record.getTID(), sid);
        addTransaction(ta);
        return;
      }
      }
      break;
    }
    super.replay(sid, record);
  }

  @Override
  protected void recover(MVTOTransaction<K> ta) {
    try {
      switch (ta.getState()) {
      case JOINED: {
        MVTOXATransaction<K> xta = (MVTOXATransaction<K>) ta;
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
        MVTOXATransaction<K> xta = (MVTOXATransaction<K>) ta;
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
