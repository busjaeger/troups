package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.Constants.DEFAULT_LOG_TABLE_NAME;
import static edu.illinois.troups.Constants.LOG_TABLE_NAME;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class HTableLogStore implements GroupLogStore {

  private final HTable logTable;
  private final byte[] tableName;
  private final byte[] familyName;

  public HTableLogStore(HTable logTable, byte[] tableName) {
    this.logTable = logTable;
    this.tableName = tableName;
    this.familyName = toBytes(logTable.getConfiguration().get(
        LOG_TABLE_NAME, DEFAULT_LOG_TABLE_NAME));
  }

  @Override
  public NavigableMap<Long, byte[]> open(HKey groupKey) throws IOException {
    Get get = new Get(groupKey.getRow());
    get.addColumn(familyName, tableName);
    Result result = logTable.get(get);
    NavigableMap<Long, byte[]> records = new TreeMap<Long, byte[]>();
    if (result.getMap() != null) {
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = result
          .getMap().get(familyName);
      if (columns != null) {
        records = columns.get(tableName);
      }
    }
    return records;
  }

  @Override
  public void append(HKey groupKey, long sid, byte[] value) throws IOException {
    Put put = new Put(groupKey.getRow(), sid);
    put.add(familyName, tableName, value);
    logTable.put(put);
  }

  @Override
  public void truncate(HKey groupKey, long sid) throws IOException {
    Delete delete = new Delete(groupKey.getRow());
    delete.deleteColumns(familyName, tableName, sid);
    logTable.delete(delete);
  }

}
