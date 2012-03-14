package edu.illinois.htx.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.tm.TransactionAbortedException;

public class TransactionManager {

  private final edu.illinois.htx.tm.TransactionManagerInterface tm;

  public TransactionManager(Configuration config) throws IOException {
    this.tm = null;// TODO lookup
  }

  public Transaction begin() {
    return new Transaction(tm.begin(), this);
  }

  public void rollback(Transaction ta) {
    tm.abort(ta.ts);
  }

  public void commit(Transaction ta) throws TransactionAbortedException {
    tm.commit(ta.ts);
  }

}
