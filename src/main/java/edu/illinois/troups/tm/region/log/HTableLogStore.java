package edu.illinois.troups.tm.region.log;

import static edu.illinois.troups.Constants.DEFAULT_LOG_TABLE_NAME;
import static edu.illinois.troups.Constants.LOG_TABLE_NAME;
import static edu.illinois.troups.tm.region.HRegionTransactionManager.demandTable;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.mortbay.log.Log;

import edu.illinois.troups.tm.region.HKey;

public class HTableLogStore implements GroupLogStore {

  public static HTableLogStore newInstance(HTablePool tablePool,
      Configuration conf, byte[] tableName) throws IOException {
    byte[] logTableName = toBytes(conf.get(LOG_TABLE_NAME,
        DEFAULT_LOG_TABLE_NAME));
    byte[] logFamilyName = toBytes(conf.get(LOG_TABLE_NAME,
        DEFAULT_LOG_TABLE_NAME));
    demandTable(conf, logTableName, logFamilyName);
    return new HTableLogStore(tablePool, logTableName, logFamilyName, tableName);
  }

  private final HTablePool tablePool;
  private final byte[] logTableName;
  private final byte[] logFamilyName;
  private final byte[] tableName;

  public HTableLogStore(HTablePool tablePool, byte[] logTableName,
      byte[] logFamilyName, byte[] tableName) {
    this.tablePool = tablePool;
    this.logTableName = logTableName;
    this.logFamilyName = logFamilyName;
    this.tableName = tableName;
  }

  @Override
  public NavigableMap<Long, byte[]> open(HKey groupKey) throws IOException {
    HTableInterface logTable = tablePool.getTable(logTableName);
    try {
      Get get = new Get(groupKey.getRow());
      get.addColumn(logFamilyName, tableName);
      Result result = logTable.get(get);
      NavigableMap<Long, byte[]> records = new TreeMap<Long, byte[]>();
      if (result.getMap() != null) {
        NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = result
            .getMap().get(logFamilyName);
        if (columns != null) {
          records = columns.get(tableName);
        }
      }
      return records;
    } finally {
      logTable.close();
    }
  }

  @Override
  public void append(HKey groupKey, long sid, byte[] value) throws IOException {
    HTableInterface logTable = tablePool.getTable(logTableName);
    try {
      Put put = new Put(groupKey.getRow(), sid);
      put.add(logFamilyName, tableName, value);
      Log.info("Thread: " + Thread.currentThread().toString() + " Log Table: "
          + logTable.hashCode());
      logTable.put(put);
    } finally {
      logTable.close();
    }
  }

  @Override
  public void truncate(HKey groupKey, long sid) throws IOException {
    HTableInterface logTable = tablePool.getTable(logTableName);
    try {
      Delete delete = new Delete(groupKey.getRow());
      delete.deleteColumns(logFamilyName, tableName, sid);
      logTable.delete(delete);
    } finally {
      logTable.close();
    }
  }

}
