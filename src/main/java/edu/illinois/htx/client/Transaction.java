package edu.illinois.htx.client;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.TransactionManagerProtocol;

/**
 * Client-side representation of a transaction
 */
public class Transaction {

  final long id;

  HTable htable;
  byte[] row;

  Transaction(long id) {
    this.id = id;
  }

  void enlist(HTable htable, byte[] row) {
    // TODO fix
    if (htable != null && htable != this.htable)
      throw new IllegalStateException(
          "Currently no support for cross-table transactions");
    if (this.htable == null) {
      this.htable = htable;
      // TODO assumes right now that all rows are hosted by same region
      this.row = row;
      TransactionManagerProtocol tm = htable.coprocessorProxy(
          TransactionManagerProtocol.class, row);
      tm.begin(id);
    }
  }

  public void rollback() {
    TransactionManagerProtocol tm = htable.coprocessorProxy(
        TransactionManagerProtocol.class, row);
    tm.abort(id);
  }

  public void commit() throws TransactionAbortedException {
    TransactionManagerProtocol tm = htable.coprocessorProxy(
        TransactionManagerProtocol.class, row);
    tm.commit(id);
  }

}
