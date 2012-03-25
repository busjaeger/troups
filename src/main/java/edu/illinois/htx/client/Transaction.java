package edu.illinois.htx.client;

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

  void enlist(HTable hTable, byte[] row) {
    // TODO support distributed TAs
    if (!(this.hTable == null || this.hTable == hTable))
      throw new IllegalStateException(
          "Currently no support for cross-table transactions");
    if (this.hTable == null) {
      this.hTable = hTable;
      // TODO assumes right now that all rows are hosted by same region
      this.row = row;
    }
  }

  public void rollback() {
    if (hTable != null) {
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      tm.abort(id);
    }
  }

  public void commit() throws TransactionAbortedException {
    if (hTable != null) {
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      tm.commit(id);
    }
  }

}
