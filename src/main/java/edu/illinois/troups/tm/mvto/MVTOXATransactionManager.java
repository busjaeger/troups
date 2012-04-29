package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.XATransactionState.JOINED;
import static edu.illinois.troups.tm.XATransactionState.PREPARED;
import static edu.illinois.troups.tm.log.XATransactionLog.RECORD_TYPE_XA_STATE_TRANSITION;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.XATransactionLog;
import edu.illinois.troups.tm.log.TransactionLog.Record;
import edu.illinois.troups.tm.log.XATransactionLog.XAStateTransitionRecord;
import edu.illinois.troups.tm.XATransactionManager;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class MVTOXATransactionManager<K extends Key, R extends Record<K>>
    extends MVTOTransactionManager<K, R> implements XATransactionManager {

  private static final Log LOG = LogFactory
      .getLog(MVTOXATransactionManager.class);

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
      reclaimLock.readLock().lock();
      try {
        // if the transaction is reclaimed, don't allow it in
        if (timestampManager.compare(tid.getTS(), reclaimed) < 0)
          throw new IllegalArgumentException("Already reclaimed " + tid);
        MVTOXATransaction<K> ta = new MVTOXATransaction<K>(this);
        ta.join(tid);
        addTransaction(ta);
        return ta.getID();
      } finally {
        reclaimLock.readLock().unlock();
      }
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void prepare(final XID xid) throws IOException {
    new IfRunning() {
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
      new IfRunning() {
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
        MVTOXATransaction<K> xta = new MVTOXATransaction<K>(this);
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
    switch (ta.getState()) {
    case JOINED: {
      // TODO conditions where we should abort here?
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
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Commit prepared tran failed during recovery " + ta, e);
      }
      // TODO conditions where we should abort here?
      return;
    }
    }
    super.recover(ta);
  }

  @Override
  protected boolean reclaim(MVTOTransaction<K> ta) {
    switch (ta.getState()) {
    case JOINED:
      return false;
    case PREPARED:
      MVTOXATransaction<K> xta = (MVTOXATransaction<K>) ta;
      XID xid = xta.getID();
      try {
        if (getTimestampManager().isReferencePersisted(xid.getTS(),
            xid.getPid())) {
          xta.commit();
          return reclaim(ta);
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Commit prepared tran failed during recovery " + ta, e);
      }
      return false;
    }
    return super.reclaim(ta);
  }

}
