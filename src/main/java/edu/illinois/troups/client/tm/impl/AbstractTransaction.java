package edu.illinois.troups.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.WritableUtils;

import edu.illinois.troups.client.tm.RowGroupPolicy;
import edu.illinois.troups.client.tm.Transaction;
import edu.illinois.troups.tm.TID;

public abstract class AbstractTransaction implements Transaction {

  @Override
  public Get enlistGet(HTable table, RowGroupPolicy policy, byte[] row) {
    TID tid = getTID(table, policy, row);
    Get get = new Get(row);
    try {
      // TODO shouldn't set 0 here
      get.setTimeRange(0L, tid.getTS());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    setTID(get, tid);
    return get;
  }

  @Override
  public Put enlistPut(HTable table, RowGroupPolicy policy, byte[] row) {
    TID tid = getTID(table, policy, row);
    Put put = new Put(row, tid.getTS());
    setTID(put, tid);
    return put;
  }

  protected void setTID(OperationWithAttributes operation, TID tid) {
    byte[] tsBytes = WritableUtils.toByteArray(tid);
    operation.setAttribute(getTIDAttr(), tsBytes);
  }

  protected abstract TID getTID(HTable table, RowGroupPolicy policy, byte[] row);

  protected abstract String getTIDAttr();

}
