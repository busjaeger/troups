package edu.illinois.troup.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import edu.illinois.troup.Constants;
import edu.illinois.troup.client.tm.Transaction;
import edu.illinois.troup.tm.TID;

public abstract class AbstractTransaction implements Transaction {

  @Override
  public Put createDelete(HTable table, byte[] row) throws IOException {
    Put put = createPut(table, row);
    put.setAttribute(Constants.ATTR_NAME_DEL, Bytes.toBytes(true));
    return put;
  }

  @Override
  public Get createGet(HTable table, byte[] row) throws IOException {
    TID tid = getTID(table, row);
    Get get = new Get(row);
    get.setTimeRange(0L, tid.getTS());
    setTID(get, tid);
    return get;
  }

  @Override
  public Put createPut(HTable table, byte[] row) throws IOException {
    TID tid = getTID(table, row);
    Put put = new Put(row, tid.getTS());
    setTID(put, tid);
    return put;
  }

  protected void setTID(OperationWithAttributes operation, TID tid) {
    byte[] tsBytes = WritableUtils.toByteArray(tid);
    operation.setAttribute(getTIDAttr(), tsBytes);
  }

  protected abstract TID getTID(HTable table, byte[] row) throws IOException;

  protected abstract String getTIDAttr();

}
