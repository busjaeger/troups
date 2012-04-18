package edu.illinois.htx.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.HTXConstants;
import edu.illinois.htx.client.transactions.Transaction;

public class HTXTable implements Closeable {

  final HTable hTable;

  public HTXTable(Configuration conf, byte[] tableName) throws IOException {
    this.hTable = new HTable(conf, tableName);
  }

  public HTXTable(Configuration conf, String tableName) throws IOException {
    this.hTable = new HTable(conf, tableName);
  }

  public Result get(Transaction ta, Get get) throws IOException {
    byte[] row = get.getRow();
    long tid = ta.enlist(hTable, row);
    org.apache.hadoop.hbase.client.Get hGet = new org.apache.hadoop.hbase.client.Get(
        row);
    setTransaction(hGet, tid);
    hGet.setTimeRange(0L, tid);
    for (Entry<byte[], ? extends Iterable<byte[]>> entry : get.getFamilyMap()
        .entrySet())
      for (byte[] column : entry.getValue())
        hGet.addColumn(entry.getKey(), column);
    // convert result into non-versioned result
    org.apache.hadoop.hbase.client.Result result = hTable.get(hGet);
    return new Result(result.getNoVersionMap());
  }

  public void put(Transaction ta, Put put) throws IOException {
    org.apache.hadoop.hbase.client.Put hPut = createTransactionPut(put, ta);
    for (List<KeyValue> kvl : put.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
    hTable.put(hPut);
  }

  /*
   * Note that deletes are transformed into put(null). This is because we cannot
   * actually remove data for a given version, because the current transaction
   * may abort. To make sure the TM treats this as a delete so that it can
   * prevent clients from reading the cell, the put is annotated with a delete
   * marker that is interpreted by our Coprocessor.
   */
  public void delete(Transaction ta, Delete delete) throws IOException {
    org.apache.hadoop.hbase.client.Put hPut = createTransactionPut(delete, ta);
    hPut.setAttribute(HTXConstants.ATTR_NAME_DEL, Bytes.toBytes(true));
    for (List<KeyValue> kvl : delete.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), null);
    hTable.put(hPut);
  }

  private org.apache.hadoop.hbase.client.Put createTransactionPut(
      Mutation mutation, Transaction ta) throws IOException {
    byte[] row = mutation.getRow();
    long tid = ta.enlist(hTable, row);
    org.apache.hadoop.hbase.client.Put hPut = new org.apache.hadoop.hbase.client.Put(
        row, tid);
    setTransaction(hPut, tid);
    return hPut;
  }

  private static void setTransaction(OperationWithAttributes operation, long tid) {
    byte[] tsBytes = Bytes.toBytes(tid);
    operation.setAttribute(HTXConstants.ATTR_NAME_TID, tsBytes);
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}
