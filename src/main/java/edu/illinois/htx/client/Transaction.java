package edu.illinois.htx.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.regionserver.HRegionTransactionManagerProtocol;
import edu.illinois.htx.tm.TransactionAbortedException;

/**
 * Client-side representation of a transaction
 */
public class Transaction {

  final long id;

  HTable hTable;
  byte[] row;

  Transaction(long id) {
    this.id = id;
  }

  /**
   * 
   * @param hTable
   * @param row
   * @return true if this is the first to enlist
   */
  boolean enlist(HTable hTable, byte[] row) {
    // TODO support distributed TAs
    if (!(this.hTable == null || this.hTable == hTable))
      throw new IllegalStateException(
          "Currently no support for cross-table transactions");
    if (this.hTable == null) {
      this.hTable = hTable;
      // TODO assumes right now that all rows are hosted by same region
      this.row = row;
      return true;
    }
    return false;
  }

  public void rollback() {
    if (hTable != null) {
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      try {
        tm.abort(id);
      } catch (IOException e) {
        throw new RuntimeException("Failed to rollback", e);
      }
    }
  }

  public void commit() throws TransactionAbortedException {
    if (hTable != null) {
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      try {
        tm.commit(id);
      } catch (IOException e) {
        throw new RuntimeException("Failed to commit", e);
      }
    }
  }

}
