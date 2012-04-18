package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.XAMVTOTransaction.XAState.CREATED;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.LogRecord.Type;
import edu.illinois.htx.tm.TransactionAbortedException;

public class XAMVTOTransaction<K extends Key> extends MVTOTransaction<K> {

  static enum XAState {
    CREATED, JOINED, PREPARED
  }

  private long pid;
  private XAState xaState;

  public XAMVTOTransaction(XAMVTOTransactionManager<K, ?> tm)
      throws IOException {
    super(tm);
    xaState = CREATED;
  }

  XAMVTOTransactionManager<K, ?> getTM() {
    return (XAMVTOTransactionManager<K, ?>) tm;
  }

  @Override
  protected void doBegin() throws IOException {
    throw newISA("Cannot begin XA transaction");
  }

  public synchronized void join(long id) throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ABORTED:
    case ACTIVE:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("join");
    }

    setID(id);
    setPID(this.pid = getTM().getTimestampManager().join(id));
    // fail here: we don't know that we joined, but client will abort
    setSID(getTM().getTransactionLog().append(Type.JOIN, id));
    setState(State.ACTIVE);
    xaState = XAState.JOINED;
  }

  public synchronized void prepare() throws TransactionAbortedException,
      IOException {
    switch (state) {
    case ACTIVE:
      break;
    case CREATED:
    case ABORTED:
    case BLOCKED:
    case COMMITTED:
    case FINALIZED:
      throw newISA("prepare");
    }
    switch (xaState) {
    case JOINED:
      break;
    case CREATED:
    case PREPARED:
      throw newISA("prepare");
    }

    waitUntilReadFromEmpty();
    getTM().getTimestampManager().prepared(id, pid);
    // fail here -> we don't know that we prepared, but we can check TSM upon
    // restart. I.e. if we see 'JOINED' in the log, we can check if TA is
    // committing
    getTM().getTransactionLog().append(Type.PREPARE, id);
    xaState = XAState.PREPARED;
  }

  @Override
  protected void aborted() throws IOException {
    getTM().getTimestampManager().aborted(id, pid);
  }

  @Override
  protected void committed() throws IOException {
    getTM().getTimestampManager().committed(id, pid);
  }

  synchronized long getPID() {
    if (state == State.CREATED)
      throw newISA("getPID");
    return this.pid;
  }

  synchronized void setPID(long pid) {
    this.pid = pid;
  }
}
