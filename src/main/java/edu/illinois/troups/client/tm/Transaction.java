package edu.illinois.troups.client.tm;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import edu.illinois.troups.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  Put enlistPut(HTable table, byte[] row) throws IOException;

  Get enlistGet(HTable table, byte[] row) throws IOException;

  Put enlistDelete(HTable table, byte[] row) throws IOException;

  void rollback();

  void commit() throws TransactionAbortedException;

}
