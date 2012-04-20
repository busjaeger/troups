package edu.illinois.htx.tm.impl;

import static edu.illinois.htx.tm.XATransactionState.JOINED;
import static edu.illinois.htx.tm.XATransactionState.PREPARED;
import static edu.illinois.htx.tm.log.Log.RECORD_TYPE_STATE_TRANSITION;
import static edu.illinois.htx.tm.log.XALog.RECORD_TYPE_JOIN;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.XATransactionManager;
import edu.illinois.htx.tm.log.JoinLogRecord;
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
  public synchronized long join(final long tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      XAMVTOTransaction<K> ta = new XAMVTOTransaction<K>(this);
      ta.join(tid);
      addTransaction(ta);
      return ta.getPID();
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void prepare(final long tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        if (!(ta instanceof XAMVTOTransaction))
          throw new IllegalStateException("Cannot prepare non XA transaction");
        ((XAMVTOTransaction<K>) ta).prepare();
        System.out.println("Prepared " + tid);
      }
    }.run(tid);
  }

  @Override
  protected void replay(LogRecord record) {
    long tid = record.getTID();
    MVTOTransaction<K> ta = getTransaction(tid);
    switch (record.getType()) {
    case RECORD_TYPE_STATE_TRANSITION:
      StateTransitionLogRecord stlr = (StateTransitionLogRecord) record;
      switch (stlr.getTransactionState()) {
      case PREPARED:
        if (ta == null)
          return;
        if (!(ta instanceof XAMVTOTransaction))
          throw new IllegalStateException(
              "prepare log record for non-XA transaction");
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        xta.setPrepared();
        return;
      }
      break;
    case RECORD_TYPE_JOIN:
      if (ta != null)
        throw new IllegalStateException("join record for existing transaction");
      JoinLogRecord jlr = (JoinLogRecord) record;
      XAMVTOTransaction<K> xta = new XAMVTOTransaction<K>(this);
      long pid = jlr.getPID();
      long sid = jlr.getSID();
      xta.setJoined(tid, pid, sid);
      addTransaction(ta);
      return;
    }
    super.replay(record);
  }

  @Override
  protected void recover(MVTOTransaction<K> ta) {
    try {
      switch (ta.getState()) {
      case JOINED: {
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        long tid = xta.getID();
        long pid = xta.getPID();
        try {
          // still a chance to complete this transaction, could also just abort
          if (!getTimestampManager().isReleased(tid)
              && getTimestampManager().isReferenceHeldByMe(tid, pid))
            return;
        } catch (NoSuchTimestampException e) {
          // fall through
        }
        xta.abort();
        return;
      }
      case PREPARED: {
        XAMVTOTransaction<K> xta = (XAMVTOTransaction<K>) ta;
        long tid = xta.getID();
        long pid = xta.getPID();
        // first try to figure out if we need to commit
        try {
          if (getTimestampManager().isReferencePersisted(tid, pid)) {
            xta.commit();
            return;
          }
        } catch (NoSuchTimestampException e) {
          // fall through
        }
        // if not, check if the transaction is still active, could also abort
        try {
          if (!getTimestampManager().isReleased(tid)
              && getTimestampManager().isReferenceHeldByMe(tid, pid))
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
