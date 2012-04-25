package edu.illinois.troup.tm.region;

import static edu.illinois.troup.Constants.DEFAULT_TM_LOG_DISABLE_TRUNCATION;
import static edu.illinois.troup.Constants.TM_LOG_DISABLE_TRUNCATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.log.Log;

public class HRegionLog implements Log<HKey, HLogRecord> {

  private final HRegionInfo regionInfo;
  // don't need to close log table, since we are using our own connection
  private final HTable logTable;
  private final byte[] family;
  private final boolean tuncateDisabled;
  private final AtomicLong sid;

  HRegionLog(HTable table, byte[] family, ExecutorService pool, HRegion region) {
    this.regionInfo = region.getRegionInfo();
    this.family = family;
    this.logTable = table;
    this.tuncateDisabled = region.getConf().getBoolean(
        TM_LOG_DISABLE_TRUNCATION, DEFAULT_TM_LOG_DISABLE_TRUNCATION);
    this.sid = new AtomicLong(0);
  }

  @Override
  public Iterable<HLogRecord> recover() throws IOException {
    Scan scan = createScan();
    byte[] table = regionInfo.getTableName();
    scan.addColumn(family, table);
    ResultScanner scanner = logTable.getScanner(scan);
    SortedSet<HLogRecord> records = new TreeSet<HLogRecord>();
    for (Result result : scanner) {
      NavigableMap<Long, byte[]> cells = result.getMap().get(family).get(table);
      for (byte[] rawRecord : cells.values()) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(rawRecord, rawRecord.length);
        int type = in.readInt();
        HLogRecord record = create(type);
        record.readFields(in);
        in.close();
        record.setGroupKey(new HKey(result.getRow()));
        records.add(record);
      }
    }
    if (!records.isEmpty())
      sid.set(records.last().getSID());
    return records;
  }

  protected HLogRecord create(int type) {
    switch (type) {
    case Log.RECORD_TYPE_STATE_TRANSITION:
      return new HStateTransitionLogRecord();
    case Log.RECORD_TYPE_GET:
      return new HGetLogRecord();
    case Log.RECORD_TYPE_DELETE:
      return new HDeleteLogRecord();
    case Log.RECORD_TYPE_PUT:
      return new HPutLogRecord();
    default:
      throw new IllegalStateException("Unknown log record type: " + type);
    }
  }

  protected long nextSID() {
    return sid.getAndIncrement();
  }

  @Override
  public long appendStateTransition(TID tid, HKey groupKey, int state)
      throws IOException {
    HLogRecord record = new HStateTransitionLogRecord(nextSID(), tid, groupKey,
        state);
    return append(record);
  }

  @Override
  public long appendGet(TID tid, HKey groupKey, HKey key, long version)
      throws IOException {
    HLogRecord record = new HGetLogRecord(nextSID(), tid, groupKey, key,
        version);
    return append(record);
  }

  @Override
  public long appendPut(TID tid, HKey groupKey, HKey key) throws IOException {
    HLogRecord record = new HPutLogRecord(nextSID(), tid, groupKey, key);
    return append(record);
  }

  @Override
  public long appendDelete(TID tid, HKey groupKey, HKey key) throws IOException {
    HLogRecord record = new HDeleteLogRecord(nextSID(), tid, groupKey, key);
    return append(record);
  }

  protected long append(HLogRecord record) throws IOException {
    // 1. serialize the log record - note: the group key is not serialized,
    // since it's already used as the log table row key
    DataOutputBuffer out = new DataOutputBuffer();
    // write type so we know which class to create during recovery
    out.writeInt(record.getType());
    record.write(out);
    out.close();
    byte[] value = out.getData();

    // 2. put to log table
    byte[] row = record.getGroupKey().getRow();
    long timestamp = record.getSID();
    Put put = new Put(row, timestamp);
    byte[] qualifier = regionInfo.getTableName();
    put.add(family, qualifier, value);
    logTable.put(put);

    // 3. return log record sequence ID
    return record.getSID();
  }

  /*
   * any better way to do this?? It seems there is no 'delete' scanner API.
   */
  @Override
  public void truncate(long sid) throws IOException {
    if (tuncateDisabled)
      return;
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

  // TODO consider overflow
  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }
}
