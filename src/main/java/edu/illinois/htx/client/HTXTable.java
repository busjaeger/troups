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
public class HTXTable implements Closeable {

  private final HTable hTable;

  public HTXTable(Configuration conf, byte[] tableName) throws IOException {
    this.hTable = new HTable(conf, tableName);
  }

  public HTXTable(Configuration conf, String tableName) throws IOException {
    this.hTable = new HTable(conf, tableName);
  }

  public Result get(Transaction ta, Get get) throws IOException {
    byte[] row = get.getRow();
    org.apache.hadoop.hbase.client.Get hGet = new org.apache.hadoop.hbase.client.Get(
        row);
    setTransactionTimestamp(ta, hGet);
    enlist(ta, row, hGet);
    hGet.setTimeRange(0L, ta.id);
    for (Entry<byte[], ? extends Iterable<byte[]>> entry : get.getFamilyMap()
        .entrySet())
      for (byte[] column : entry.getValue())
        hGet.addColumn(entry.getKey(), column);
    // convert result into non-versioned result
    org.apache.hadoop.hbase.client.Result result = hTable.get(hGet);
    return new Result(result.getNoVersionMap());
  }

  public void put(Transaction ta, Put put) throws IOException {
    byte[] row = put.getRow();
    org.apache.hadoop.hbase.client.Put hPut = new org.apache.hadoop.hbase.client.Put(
        put.getRow(), ta.id);
    enlist(ta, row, hPut);
    setTransactionTimestamp(ta, hPut);
    for (List<KeyValue> kvl : put.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
    hTable.put(hPut);
  }

  public void delete(Transaction ta, Delete delete) throws IOException {
    /*
     * Note that deletes are transformed into put(null). This is because we
     * cannot actually remove data for a given version, because the current
     * transaction may abort. To make sure the TM treats this as a delete so
     * that it can prevent clients from reading the cell, the put is annotated
     * with a delete marker that is interpreted by our co-processor.
     * 
     * TODO: support other types of deletes (columns, family)
     */
    byte[] row = delete.getRow();
    org.apache.hadoop.hbase.client.Put hPut = new org.apache.hadoop.hbase.client.Put(
        delete.getRow(), ta.id);
    enlist(ta, row, hPut);
    setTransactionTimestamp(ta, hPut);
    hPut.setAttribute(HTXConstants.ATTR_NAME_DEL, Bytes.toBytes(true));
    for (List<KeyValue> kvl : delete.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), null);
    hTable.put(hPut);
  }

  private void enlist(Transaction ta, byte[] row, OperationWithAttributes op) {
    if (ta.enlist(hTable, row))
      op.setAttribute(HTXConstants.ATTR_NAME_BEG, Bytes.toBytes(true));
  }

  private static void setTransactionTimestamp(Transaction ta,
      OperationWithAttributes operation) {
    byte[] tsBytes = Bytes.toBytes(ta.id);
    operation.setAttribute(HTXConstants.ATTR_NAME_TID, tsBytes);
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}