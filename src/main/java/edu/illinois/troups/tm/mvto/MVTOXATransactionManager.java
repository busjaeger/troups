package edu.illinois.troups.tm.mvto;

import static edu.illinois.troups.tm.TransactionState.FINALIZED;
import static edu.illinois.troups.tm.XATransactionState.JOINED;
import static edu.illinois.troups.tm.XATransactionState.PREPARED;
import static edu.illinois.troups.tm.log.XATransactionLog.RECORD_TYPE_XA_STATE_TRANSITION;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XATransactionManager;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.TransactionLog.Record;
import edu.illinois.troups.tm.log.XATransactionLog;
import edu.illinois.troups.tm.log.XATransactionLog.XAStateTransitionRecord;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class MVTOXATransactionManager<K extends Key, R extends Record<K>>
    extends MVTOTransactionManager<K, R> implements XATransactionManager {

  private static final Log LOG = LogFactory
      .getLog(MVTOXATransactionManager.class);

  public MVTOXATransactionManager(KeyValueStore<K> keyValueStore,
      XATransactionLog<K, R> log, SharedTimestampManager timestampManager,
      ExecutorService pool) {
    super(keyValueStore, log, timestampManager, pool);
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
  public XID join(final TID tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      reclaimLock.readLock().lock();
      try {
        long ts = tid.getTS();
        /*
         * only allow in transactions that started before the first active
         * transaction. Otherwise, we may not be able to implement the
         * concurrency control protocol correctly, in case we already discarded
         * transactions that follow this one.
         */
        if (timestampManager.compare(ts, lastReclaimed) <= 0)
          throw new TransactionAbortedException("Already reclaimed " + tid);

        long pid;
        try {
          pid = getTimestampManager().acquireReference(ts);
        } catch (NoSuchTimestampException e) {
          throw new TransactionAbortedException("Already reclaimed " + tid, e);
        }

        XID xid = new XID(ts, pid);
        int state = JOINED;
        long sid = getTransactionLog().appendXAStateTransition(xid, state);

        // do not support joining the same transaction twice
        MVTOXATransaction<K> ta = new MVTOXATransaction<K>(this, xid, sid,
            JOINED);
        if (transactions.putIfAbsent(xid, ta) != null) {
          getTimestampManager().releaseReference(ts, pid);
          getTransactionLog().appendStateTransition(xid, FINALIZED);
          throw new TransactionAbortedException("Branch for transaction " + tid
              + " already running");
        }

        return xid;
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
          throw new IllegalStateException("Prepare for non XA transaction");
        ((MVTOXATransaction<K>) ta).prepare();
        LOG.debug("Prepared " + ta);
      }
    }.run(xid);
  }

  @Override
  public void commit(XID xid, boolean onePhase) throws IOException {
    if (onePhase) {
      new IfRunning() {
        @Override
        void execute(MVTOTransaction<K> ta) throws IOException {
          if (ta instanceof MVTOXATransaction)
            ((MVTOXATransaction<K>) ta).setPrepared();
          ta.commit();
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
      case JOINED: {
        if (ta != null)
          throw new IllegalStateException(
              "join record for existing transaction");
        XID xid = stlr.getTID();
        transactions.put(xid, new MVTOXATransaction<K>(this, xid, sid, JOINED));
        return;
      }
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
          LOG.info("Recovery committing prepared transaction " + ta);
          xta.commit();
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Commit prepared tran failed during recovery " + ta, e);
        e.printStackTrace(System.out);
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
          LOG.info("Reclaim committing prepared transaction " + ta);
          xta.commit();
          return reclaim(ta);
        }
      } catch (NoSuchTimestampException e) {
        // fall through
      } catch (IOException e) {
        LOG.error("Commit prepared tran failed during recovery " + ta, e);
        e.printStackTrace(System.out);
      }
      return false;
    }
    return super.reclaim(ta);
  }

}
