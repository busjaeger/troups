package edu.illinois.htx.tm.mvto;

import java.io.IOException;

import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.XATransactionManager;
import edu.illinois.htx.tm.mvto.MVTOTransactionManager.WithReadLock;

public class XAMVTOTransactionManager implements XATransactionManager {

  @Override
  public synchronized void join(final long tid) throws IOException {
    runLock.readLock().lock();
    try {
      checkRunning();
      createTransaction(tid);
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

  @Override
  public void abort(long tid) throws IOException {
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
