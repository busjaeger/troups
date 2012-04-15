package edu.illinois.htx.regionserver;

import static edu.illinois.htx.HTXConstants.DEFAULT_TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.htx.HTXConstants.DEFAULT_TM_LOG_TABLE_NAME;
import static edu.illinois.htx.HTXConstants.TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.htx.HTXConstants.TM_LOG_TABLE_NAME;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import edu.illinois.htx.tm.AbstractLog;
import edu.illinois.htx.tm.LogRecord.Type;

// TODO start/stop behavior
public class HRegionLog extends AbstractLog<HKey, HLogRecord> {

  public static HRegionLog newInstance(HConnection connection,
      ExecutorService pool, HRegionInfo regionInfo) throws IOException {
    Configuration config = connection.getConfiguration();
    byte[] tableName = toBytes(config.get(TM_LOG_TABLE_NAME,
        DEFAULT_TM_LOG_TABLE_NAME));
    byte[] family = toBytes(config.get(TM_LOG_TABLE_FAMILY_NAME,
        DEFAULT_TM_LOG_TABLE_FAMILY_NAME));
    HTable table = new HTable(tableName, connection, pool);
    return new HRegionLog(table, family, pool, regionInfo);
  }

  private final HRegionInfo regionInfo;
  private final HTable logTable;
  private final byte[] family;
  private final AtomicLong sid;
  private final Map<Long, byte[]> rows;
  private final Map<Long, HLogRecord> begins;

  HRegionLog(HTable table, byte[] family, ExecutorService pool,
      HRegionInfo regionInfo) throws IOException {
    this.regionInfo = regionInfo;
    this.family = family;
    this.logTable = table;
    this.sid = new AtomicLong(0);
    this.rows = new ConcurrentHashMap<Long, byte[]>();
    this.begins = new ConcurrentHashMap<Long, HLogRecord>();
  }

  public Iterable<HLogRecord> start() throws IOException {
    Scan scan = createScan();
    ResultScanner scanner = logTable.getScanner(scan);
    SortedSet<HLogRecord> records = new TreeSet<HLogRecord>();
    for (Result result : scanner) {
      NavigableMap<Long, byte[]> cells = result.getMap().get(family)
          .get(regionInfo.getTableName());
      for (byte[] rawRecord : cells.values()) {
        HLogRecord record = new HLogRecord();
        DataInputBuffer in = new DataInputBuffer();
        in.reset(rawRecord, rawRecord.length);
        record.readFields(in);
        in.close();
        records.add(record);
      }
    }
    for (HLogRecord record : records) {
      switch (record.getType()) {
      case BEGIN:
        rows.put(record.getTID(), record.getKey().getRow());
        break;
      case FINALIZE:
        rows.remove(record.getTID());
        break;
      default:
        break;
      }
    }
    if (!records.isEmpty())
      sid.set(records.last().getSID());
    return records;
  }

  public void stop() throws IOException {
    // don't need to close log table, since we are using our own connection
    logTable.close();
  }

  @Override
  public HLogRecord newRecord(Type type, long tid, HKey key, long version) {
    return new HLogRecord(sid.getAndIncrement(), tid, type, key, version);
  }

  // don't append the begin, because we don't know the row yet
  @Override
  public long appendBegin(long tid) {
    HLogRecord record = newRecord(Type.BEGIN, tid, null, -1);
    begins.put(tid, record);
    return record.getSID();
  }

  @Override
  public void append(HLogRecord record) throws IOException {
    List<Put> puts = new ArrayList<Put>(2);
    HLogRecord beginRecord = begins.remove(record.getTID());
    if (beginRecord != null) {
      rows.put(record.getTID(), record.getKey().getRow());
      Put beginPut = newPut(beginRecord);
      puts.add(beginPut);
    }
    puts.add(newPut(record));
    logTable.put(puts);
    if (record.getType() == Type.FINALIZE)
      rows.remove(record.getTID());
  }

  /*
   * any better way to do this?? It seems there is no 'delete' scanner API.
   */
  @Override
  public void savepoint(long sid) throws IOException {
    Scan scan = createScan();
    scan.setTimeRange(0L, sid);
    ResultScanner scanner = logTable.getScanner(scan);
    List<Delete> deletes = new ArrayList<Delete>();
    for (Result result : scanner) {
      Delete delete = new Delete(result.getRow());
      delete.deleteColumns(family, regionInfo.getTableName(), sid);
      deletes.add(delete);
    }
    logTable.delete(deletes);
  }

  private Scan createScan() {
    Scan scan = new Scan();
    scan.setStartRow(regionInfo.getStartKey());
    scan.setStopRow(regionInfo.getEndKey());
    scan.addFamily(family);
    return scan;
  }

  private Put newPut(HLogRecord record) {
    byte[] row = rows.get(record.getTID());
    long timestamp = record.getSID();
    byte[] qualifier = regionInfo.getTableName();
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      record.write(buffer);
    } catch (IOException e) {
      // shouldn't happen, we are writing into memory
      throw new RuntimeException(e);
    }
    byte[] value = buffer.getData();
    Put put = new Put(row, timestamp);
    put.add(family, qualifier, value);
    return put;
  }
}
