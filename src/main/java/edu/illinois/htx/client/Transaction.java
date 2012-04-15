package edu.illinois.htx.client;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  public long getID();

  public void enlist(HTable table, byte[] row);

  public void rollback();

  public void commit() throws TransactionAbortedException;

}
