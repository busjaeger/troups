package edu.illinois.htx.client.impl;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.tm.TransactionAbortedException;

public class DynamicTransaction implements Transaction {

  @Override
  public long getID() {
    return 0;
  }

  @Override
  public void enlist(HTable table, byte[] row) {
  }

  @Override
  public void rollback() {
  }

  @Override
  public void commit() throws TransactionAbortedException {
  }

}
