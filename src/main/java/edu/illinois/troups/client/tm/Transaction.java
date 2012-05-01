package edu.illinois.troups.client.tm;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import edu.illinois.troups.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  Put enlistPut(HTable table, RowGroupPolicy policy, byte[] row)
      throws TransactionAbortedException;

  Get enlistGet(HTable table, RowGroupPolicy policy, byte[] row)
      throws TransactionAbortedException;

  void rollback();

  void commit() throws TransactionAbortedException;

}
