package edu.illinois.troups.tm.region.log;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import edu.illinois.troups.tm.region.HKey;

public class HTableLogStore implements GroupLogStore {

  private final HTable logTable;
  private final byte[] tableName;
  private final byte[] familyName;

  public HTableLogStore(HTable logTable, byte[] logFamilyName, byte[] tableName) {
    this.logTable = logTable;
    this.tableName = tableName;
    this.familyName = logFamilyName;
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
