package edu.illinois.htx.tm.mvto;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.LogRecord.Type;
import edu.illinois.htx.tm.TransactionAbortedException;

public class XAMVTOTransaction<K extends Key> extends MVTOTransaction<K> {

  static enum ParticipantState {
    JOINED, PREPARED
  }

  private long pid;

  public XAMVTOTransaction(XAMVTOTransactionManager<K, ?> tm) throws IOException {
    super(tm);
  }

  XAMVTOTransactionManager<K, ?> getTM() {
    return (XAMVTOTransactionManager<K, ?>) tm;
  }
  
  public synchronized long join() throws IOException {
    switch(state) {
    case CREATED:
      break;
    case ABORTED:
    case ACTIVE:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("join");
    }
    long pid = getTM().getTimestampManager().join(id);
    getTM().getTransactionLog().append(Type., id);
    
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    switch (state) {
    case PREPARED:
      return;
    case ACTIVE:
    case ABORTED:
    case BLOCKED:
    case PRE_COMMITTED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("prepare");
    }
    waitForWriters();
    tm.getTimestampManager().prepared(id, pid);
    tm.getTransactionLog().appendStateChange(id, PREPARED);
    state = PREPARED;
  }

  synchronized long getPID() {
    return this.pid;
  }

}
