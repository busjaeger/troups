package edu.illinois.htx.tm.mvto;

import static edu.illinois.htx.tm.mvto.XAMVTOTransaction.XAState.CREATED;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.LogRecord.Type;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.SharedTimestampManager;
import edu.illinois.htx.tsm.TimestampManager.TimestampListener;

public class XAMVTOTransaction<K extends Key> extends MVTOTransaction<K> implements TimestampListener {

  static enum XAState {
    CREATED, JOINED, PREPARED
  }

  private long pid;
  private XAState xaState;

  public XAMVTOTransaction(XAMVTOTransactionManager<K, ?> tm) {
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
    long pid = getTimestampManager().createReference(id);
    getTimestampManager().addReferenceListener(id, 0, this);
    setID(id);
    setPID(pid);
    // if we fail here, TSM will clean up reference and client will abort
    setSID(getTransactionLog().append(Type.JOIN, id, getPID()));
    setState(State.ACTIVE);
    setXAState(XAState.JOINED);
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
    getTransactionLog().append(Type.PREPARE, id);
    setXAState(XAState.PREPARED);
  }

  /**
   * Client crashed
   *
   * @param ts
   */
  @Override
  public void deleted(long ts) {
    
  }

  @Override
  protected void afterFinalize() throws IOException {
    getTimestampManager().releaseReference(id, pid);
  }

  @Override
  protected SharedTimestampManager getTimestampManager() {
    return ((XAMVTOTransactionManager<K, ?>) tm).getTimestampManager();
  }

  synchronized void setXAState(XAState xaState) {
    this.xaState = xaState;
  }

  synchronized XAState getXAState() {
    return xaState;
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
