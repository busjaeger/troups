package edu.illinois.troup.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troup.Constants;
import edu.illinois.troup.client.tm.RowGroupPolicy;
import edu.illinois.troup.client.tm.Transaction;
import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.TransactionAbortedException;
import edu.illinois.troup.tm.region.HKey;
import edu.illinois.troup.tm.region.RTM;

public class GroupTransaction extends AbstractTransaction implements
    Transaction {

  private TID id;
  private RowGroupPolicy groupPolicy;
  private HTable table;
  private byte[] row;
  private boolean completed = false;

  GroupTransaction() {
    super();
  }

  @Override
  public TID getTID(HTable table, byte[] row) throws IOException {
    if (completed)
      throw new IllegalStateException("Already completed");

    // if this is the first enlist -> begin transaction
    if (this.table == null) {
      RowGroupPolicy groupPolicy = RowGroupSplitPolicy
          .getRowGroupStrategy(table);
      if (groupPolicy != null)
        row = groupPolicy.getGroupKey(row);
      RTM rtm = table.coprocessorProxy(RTM.class, row);
      this.id = rtm.begin(new HKey(row));
      this.table = table;
      this.row = row;
      this.groupPolicy = groupPolicy;
    }
    // otherwise ensure this transaction remains local
    else {
      // check same table
      if (!Bytes.equals(this.table.getTableName(), table.getTableName()))
        throw new IllegalArgumentException(
            "Local transaction cannot span tables");
      // check same row group
      if (groupPolicy != null)
        row = groupPolicy.getGroupKey(row);
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
