package edu.illinois.htx.client.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.regionserver.RTM;
import edu.illinois.htx.tm.TransactionAbortedException;

// TODO think about IOException during being/abort/commit
public class LocalTransaction implements Transaction {

  private HTable table;
  private byte[] row;
  private long id;
  private boolean completed = false;

  LocalTransaction() {
    super();
  }

  /**
   * 
   * @param table
   * @param row
   * @return true if this is the first to enlist
   * @throws IOException
   */
  @Override
  public long enlist(HTable table, byte[] row) throws IOException {
    if (completed)
      throw new IllegalStateException("Already completed");

    if (this.table == null) {
      // begin transaction
      RTM rtm = table.coprocessorProxy(RTM.class, row);
      this.id = rtm.begin();
      this.table = table;
      this.row = row;
    } else if (!Bytes.equals(this.table.getTableName(), table.getTableName())) {
      throw new IllegalStateException("Local transaction cannot span tables");
    }
    // TODO check row in same group if metadata present
    return id;
  }

  @Override
  public void rollback() {
    if (table != null) {
      RTM rtm = table.coprocessorProxy(RTM.class, row);
      try {
        rtm.abort(id);
      } catch (IOException e) {
        // TODO: should we just ignore this?
        throw new RuntimeException("Failed to rollback", e);
      }
    }
    completed = true;
  }

  @Override
  public void commit() throws TransactionAbortedException {
    if (table == null)
      throw new IllegalStateException("No data to commit");
    RTM rtm = table.coprocessorProxy(RTM.class, row);
    try {
      rtm.commit(id);
    } catch (IOException e) {
      throw new RuntimeException("Failed to commit", e);
    }
    completed = true;
  }

}
