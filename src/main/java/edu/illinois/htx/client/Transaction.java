package edu.illinois.htx.client;

import java.io.IOException;

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

  public void rollback() throws IOException {
    tm.rollback(this);
  }

  public void commit() throws TransactionAbortedException, IOException {
    tm.commit(this);
  }

}
