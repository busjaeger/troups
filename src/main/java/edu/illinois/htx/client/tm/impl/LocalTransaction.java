package edu.illinois.htx.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.Constants;
import edu.illinois.htx.client.tm.RowGroupPolicy;
import edu.illinois.htx.client.tm.Transaction;
import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.region.RTM;

public class LocalTransaction extends AbstractTransaction implements
    Transaction {

  private HTable table;
  private RowGroupPolicy strategy;
  private byte[] row;
  private TID id;
  private boolean completed = false;

  LocalTransaction() {
    super();
  }

  @Override
  public TID getTID(HTable table, byte[] row) throws IOException {
    if (completed)
      throw new IllegalStateException("Already completed");

    // if this is the first enlist -> begin transaction
    if (this.table == null) {
      RTM rtm = table.coprocessorProxy(RTM.class, row);
      this.id = rtm.begin();
      this.table = table;
      this.strategy = RowGroupSplitPolicy.getRowGroupStrategy(table);
      this.row = strategy == null ? row : strategy.getGroupRow(row);
    }
    // otherwise ensure this transaction remains local
    else {
      // check same table
      if (!Bytes.equals(this.table.getTableName(), table.getTableName()))
        throw new IllegalArgumentException(
            "Local transaction cannot span tables");
      // check same row group
      if (strategy != null)
        row = strategy.getGroupRow(row);
      if (!Bytes.equals(this.row, row))
        throw new IllegalArgumentException(
            "Local transaction cannot span row groups");
    }
    return id;
  }

  @Override
  protected String getTIDAttr() {
    return Constants.ATTR_NAME_TID;
  }

  @Override
  public void rollback() {
    if (table != null) {
      RTM rtm = table.coprocessorProxy(RTM.class, row);
      try {
        rtm.abort(id);
      } catch (IOException e) {
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
