package edu.illinois.htx.tm.mvto;

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
  protected void recover(LogRecord record) {
    switch (record.getType()) {
    case RECORD_TYPE_STATE_TRANSITION:
      StateTransitionLogRecord stlr = (StateTransitionLogRecord) record;
      switch (stlr.getTransactionState()) {
      case PREPARED:
        XAMVTOTransaction<K> ta = (XAMVTOTransaction<K>) getTransaction(record
            .getTID());
        ta.setState(PREPARED);
        return;
      }
      break;
    case RECORD_TYPE_JOIN:
      JoinLogRecord jlr = (JoinLogRecord) record;
      XAMVTOTransaction<K> ta = new XAMVTOTransaction<K>(this);
      ta.setID(jlr.getTID());
      ta.setPID(jlr.getPID());
      ta.setSID(jlr.getSID());
      ta.setState(JOINED);
      addTransaction(ta);
      return;
    }
    super.recover(record);
  }

}
