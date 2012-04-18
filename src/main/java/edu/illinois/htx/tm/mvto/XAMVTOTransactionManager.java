package edu.illinois.htx.tm.mvto;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.Log;
import edu.illinois.htx.tm.LogRecord;
import edu.illinois.htx.tm.XATransactionManager;
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

}
