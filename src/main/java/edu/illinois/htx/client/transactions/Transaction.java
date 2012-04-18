package edu.illinois.htx.client.transactions;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  public long enlist(HTable table, byte[] row) throws IOException;

  public void rollback();

  public void commit() throws TransactionAbortedException;

}
