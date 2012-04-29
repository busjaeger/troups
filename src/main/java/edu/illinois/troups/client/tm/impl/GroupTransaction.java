package edu.illinois.troups.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;

import edu.illinois.troups.Constants;
import edu.illinois.troups.client.tm.RowGroupPolicy;
import edu.illinois.troups.client.tm.Transaction;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.region.GroupTransactionManager;
import edu.illinois.troups.tm.region.HKey;

public class GroupTransaction extends AbstractTransaction implements
    Transaction {

  private TID id;
  private RowGroupPolicy groupPolicy;
  private RowGroup group;
  private boolean completed = false;

  GroupTransaction() {
    super();
  }

  @Override
  public TID getTID(HTable table, RowGroupPolicy policy, byte[] row)
      throws IOException {
    if (completed)
      throw new IllegalStateException("Already completed");

    // if this is the first enlist -> begin transaction
    if (this.group == null) {
      if (policy == null)
        policy = RowGroupSplitPolicy.getRowGroupStrategy(table);
      if (policy != null)
        row = policy.getGroupKey(row);
      RowGroup group = new RowGroup(table, row);
      this.id = getTM(group).begin(group.getKey());
      this.groupPolicy = policy;
      this.group = group;
    }
    // otherwise ensure this transaction remains local
    else {
      if (groupPolicy != null)
        row = groupPolicy.getGroupKey(row);
      RowGroup group = new RowGroup(table, row);
      if (!this.group.equals(group))
        throw new IllegalArgumentException(
            "Group transaction cannot span groups");
    }
    return id;
  }

  @Override
  protected String getTIDAttr() {
    return Constants.ATTR_NAME_TID;
  }

  @Override
  public void rollback() {
    if (group != null) {
      try {
        getTM(group).abort(group.getKey(), id);
      } catch (IOException e) {
        throw new RuntimeException("Failed to rollback", e);
      }
    }
    completed = true;
  }

  @Override
  public void commit() throws TransactionAbortedException {
    if (group == null)
      throw new IllegalStateException("No data to commit");
    try {
      getTM(group).commit(group.getKey(), id);
    } catch (IOException e) {
      throw new RuntimeException("Failed to commit", e);
    }
    completed = true;
  }

  @SuppressWarnings("unchecked")
  GroupTransactionManager<HKey> getTM(RowGroup group) {
    return group.getTable().coprocessorProxy(GroupTransactionManager.class,
        group.getKey().getRow());
  }

}
