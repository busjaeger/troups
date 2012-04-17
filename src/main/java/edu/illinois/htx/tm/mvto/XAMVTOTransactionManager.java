package edu.illinois.htx.tm.mvto;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.Log;
import edu.illinois.htx.tm.LogRecord;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.XATransactionManager;
import edu.illinois.htx.tm.mvto.MVTOTransactionManager.WithReadLock;
import edu.illinois.htx.tsm.TimestampManager;
import edu.illinois.htx.tsm.XATimestampManager;

public class XAMVTOTransactionManager<K extends Key, R extends LogRecord<K>>
    extends MVTOTransactionManager<K, R> implements XATransactionManager {

  public XAMVTOTransactionManager(KeyValueStore<K> keyValueStore,
      Log<K, R> log, XATimestampManager timestampManager) {
    super(keyValueStore, log, timestampManager);
  }

  @Override
  public XATimestampManager getTimestampManager() {
    return (XATimestampManager) super.getTimestampManager();
  }

  @Override
  public synchronized long join(final long tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      XAMVTOTransaction<K> ta = new XAMVTOTransaction<K>(this);
      long pid = ta.join();
      addTransaction(ta);
      return pid;
    } finally {
      runLock.readLock().unlock();
    }
  }

  @Override
  public void prepare(final long tid) throws IOException {
    new WithReadLock() {
      void execute(MVTOTransaction<K> ta) throws IOException {
        ta.prepare();
        System.out.println("Prepared " + tid);
      }
    }.run(tid);
  }

  @Override
  public void abort(long tid) throws IOException {
  }

  @Override
  public void commit(long tid) throws TransactionAbortedException, IOException {

  case PREPARED:
    tm.getLog().appendStateChange(id, PRE_COMMITTED);
    state = PRE_COMMITTED;
    tm.getTimestampManager().done(id, pid);
    break;

  case PRE_COMMITTED:
    tm.getTimestampManager().done(id, pid);
    break;
  }

  MVTOTransaction<K> createTransaction() throws IOException {
    MVTOTransaction<K> ta;
    synchronized (transactions) {
      if (transactions.containsKey(tid))
        throw new IllegalStateException("Transaction " + tid
            + " already exists");
      transactions.put(tid, ta = new MVTOTransaction<K>(tid, this));
    }
    return ta;
  }

}
