package edu.illinois.htx.client;

import edu.illinois.htx.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public class Transaction {

  final long ts;
  final TransactionManager tm;

  Transaction(long ts, TransactionManager tm) {
    this.ts = ts;
    this.tm = tm;
  }

  public void rollback() {
    tm.rollback(this);
  }

  public void commit() throws TransactionAbortedException {
    tm.commit(this);
  }

}
