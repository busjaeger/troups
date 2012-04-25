package edu.illinois.troup.client.tm;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import edu.illinois.troup.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public interface Transaction {

  Put createPut(HTable table, byte[] row) throws IOException;

  Get createGet(HTable table, byte[] row) throws IOException;

  Put createDelete(HTable table, byte[] row) throws IOException;

  void rollback();

  void commit() throws TransactionAbortedException;

}
