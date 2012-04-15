package edu.illinois.htx.client.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.HTXConstants;
import edu.illinois.htx.client.Delete;
import edu.illinois.htx.client.Get;
import edu.illinois.htx.client.HTXTable;
import edu.illinois.htx.client.KeyValue;
import edu.illinois.htx.client.Mutation;
import edu.illinois.htx.client.Put;
import edu.illinois.htx.client.Result;
import edu.illinois.htx.client.Transaction;

/**
 * <p>
 * Client view of a table stored in htx. Exposes similar operations to HTable,
 * but time-stamps are not supported at the user programming model level,
 * because htx uses them to implement MVCC.
 * </p>
 * <p>
 * HTXTable uses the HBase APIs internally to send normal operations to the
 * region servers. However, each operation is annotated with a transaction
 * time-stamp so that the observer running in the region server can recognize it
 * a transactional operation and enlist it properly.
 * </p>
 * 
 * TODO
 * <ol>
 * <li>support more of the HBase API</li>
 * <li>once we start grouping several user-level tables into one HBase table,
 * mapping needs to be added here</li>
 * </ol>
 */
public class HTXTableImpl implements HTXTable {

  final HTable hTable;

  public HTXTableImpl(Configuration conf, byte[] tableName) throws IOException {
    this.hTable = new HTable(conf, tableName); // TODO lookup mapping
  }

  public HTXTableImpl(Configuration conf, String tableName) throws IOException {
    this.hTable = new HTable(conf, tableName);
  }

  @Override
  public Result get(Transaction ta, Get get) throws IOException {
    byte[] row = get.getRow();
    ta.enlist(hTable, row);
    org.apache.hadoop.hbase.client.Get hGet = new org.apache.hadoop.hbase.client.Get(
        row);
    setTransaction(hGet, ta);
    hGet.setTimeRange(0L, ta.getID());
    for (Entry<byte[], ? extends Iterable<byte[]>> entry : get.getFamilyMap()
        .entrySet())
      for (byte[] column : entry.getValue())
        hGet.addColumn(entry.getKey(), column);
    // convert result into non-versioned result
    org.apache.hadoop.hbase.client.Result result = hTable.get(hGet);
    return new Result(result.getNoVersionMap());
  }

  @Override
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
  @Override
  public void delete(Transaction ta, Delete delete) throws IOException {
    org.apache.hadoop.hbase.client.Put hPut = createTransactionPut(delete, ta);
    hPut.setAttribute(HTXConstants.ATTR_NAME_DEL, Bytes.toBytes(true));
    // TODO: support other types of deletes (columns, family)
    for (List<KeyValue> kvl : delete.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), null);
    hTable.put(hPut);
  }

  private org.apache.hadoop.hbase.client.Put createTransactionPut(
      Mutation mutation, Transaction ta) {
    byte[] row = mutation.getRow();
    ta.enlist(hTable, row);
    long timestamp = ta.getID();
    org.apache.hadoop.hbase.client.Put hPut = new org.apache.hadoop.hbase.client.Put(
        row, timestamp);
    setTransaction(hPut, ta);
    return hPut;
  }

  private static void setTransaction(OperationWithAttributes operation,
      Transaction ta) {
    byte[] tsBytes = Bytes.toBytes(ta.getID());
    operation.setAttribute(HTXConstants.ATTR_NAME_TID, tsBytes);
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}