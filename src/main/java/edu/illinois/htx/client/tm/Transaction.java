package edu.illinois.htx.client.tm;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import edu.illinois.htx.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  Get createGet(HTable table, byte[] row) throws IOException;

  Put createPut(HTable table, byte[] row) throws IOException;

  void rollback();

  void commit() throws TransactionAbortedException;

}
