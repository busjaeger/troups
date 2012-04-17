package edu.illinois.htx.tm.mvto;

import java.io.IOException;

import edu.illinois.htx.tm.TransactionAbortedException;

public class XAMVTOTransaction {

  private final long pid;

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
    tm.getLog().appendStateChange(id, PREPARED);
    state = PREPARED;
  }


  synchronized long getPID() {
    return this.pid;
  }

}
