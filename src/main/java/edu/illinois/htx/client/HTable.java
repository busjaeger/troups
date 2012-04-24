package edu.illinois.htx.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.client.tm.Transaction;

public class HTable implements Closeable {

  final org.apache.hadoop.hbase.client.HTable hTable;

  public HTable(Configuration conf, byte[] tableName) throws IOException {
    this.hTable = new org.apache.hadoop.hbase.client.HTable(conf, tableName);
  }

  public HTable(Configuration conf, String tableName) throws IOException {
    this.hTable = new org.apache.hadoop.hbase.client.HTable(conf, tableName);
  }

  public Result get(Transaction ta, Get get) throws IOException {
    org.apache.hadoop.hbase.client.Get hGet = ta
        .createGet(hTable, get.getRow());
    for (Entry<byte[], ? extends Iterable<byte[]>> entry : get.getFamilyMap()
        .entrySet())
      for (byte[] column : entry.getValue())
        hGet.addColumn(entry.getKey(), column);
    // convert result into non-versioned result
    org.apache.hadoop.hbase.client.Result result = hTable.get(hGet);
    return new Result(result.getNoVersionMap());
  }

  public void put(Transaction ta, Put put) throws IOException {
    org.apache.hadoop.hbase.client.Put hPut = ta
        .createPut(hTable, put.getRow());
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
    org.apache.hadoop.hbase.client.Put hPut = ta.createDelete(hTable,
        delete.getRow());
    for (List<KeyValue> kvl : delete.getFamilyMap().values())
      for (KeyValue kv : kvl)
        hPut.add(kv.getFamily(), kv.getQualifier(), null);
    hTable.put(hPut);
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }
}
