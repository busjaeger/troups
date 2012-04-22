package edu.illinois.htx.tm.region;

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

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.TransactionState;
import edu.illinois.htx.tm.log.Log;
import edu.illinois.htx.tm.log.OperationLogRecord;
import edu.illinois.htx.tm.log.StateTransitionLogRecord;

public class HRegionLog implements Log<HKey, HLogRecord> {

  private final HRegion region;
  private final HRegionInfo regionInfo;
  // don't need to close log table, since we are using our own connection
  private final HTable logTable;
  private final byte[] family;
  private final AtomicLong sid;
  private final Map<TID, byte[]> rows;
  private final Map<TID, HLogRecord> begins;

  HRegionLog(HTable table, byte[] family, ExecutorService pool, HRegion region) {
    this.region = region;
    this.regionInfo = region.getRegionInfo();
    this.family = family;
    this.logTable = table;
    this.sid = new AtomicLong(0);
    this.rows = new ConcurrentHashMap<TID, byte[]>();
    this.begins = new ConcurrentHashMap<TID, HLogRecord>();
  }

  @Override
  public Iterable<HLogRecord> recover() throws IOException {
    Scan scan = createScan();
    ResultScanner scanner = logTable.getScanner(scan);
    SortedSet<HLogRecord> records = new TreeSet<HLogRecord>();
    for (Result result : scanner) {
      NavigableMap<Long, byte[]> cells = result.getMap().get(family)
          .get(regionInfo.getTableName());
      for (byte[] rawRecord : cells.values()) {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(rawRecord, rawRecord.length);
        int type = in.readInt();
        HLogRecord record = create(type);
        record.readFields(in);
        in.close();
        records.add(record);
      }
    }
    // reconstruct in-memory state
    for (HLogRecord record : records) {
      TID tid = record.getTID();
      if (isStarted(record)) {
        begins.put(record.getTID(), record);
        continue;
      }
      HLogRecord beginRecord = begins.remove(tid);
      if (beginRecord != null && beginRecord instanceof OperationLogRecord) {
        @SuppressWarnings("unchecked")
        OperationLogRecord<HKey> oplr = (OperationLogRecord<HKey>) record;
        rows.put(tid, oplr.getKey().getRow());
      }
      if (isFinalized(record)) {
        begins.remove(tid);
        rows.remove(tid);
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
  public long appendStateTransition(TID tid, int state) throws IOException {
    return append(new HStateTransitionLogRecord(nextSID(), tid, state));
  }

  @Override
  public long appendGet(TID tid, HKey key, long version) throws IOException {
    return append(new HGetLogRecord(nextSID(), tid, key, version));
  }

  @Override
  public long appendPut(TID tid, HKey key) throws IOException {
    return append(new HPutLogRecord(nextSID(), tid, key));
  }

  @Override
  public long appendDelete(TID tid, HKey key) throws IOException {
    return append(new HDeleteLogRecord(nextSID(), tid, key));
  }

  protected long append(HLogRecord record) throws IOException {
    TID tid = record.getTID();
    // don't append begin, because we don't know the row yet
    if (isStarted(record)) {
      begins.put(tid, record);
      return record.getSID();
    }

    // TODO use split policy
    List<Put> puts = new ArrayList<Put>(2);
    HLogRecord beginRecord = begins.remove(tid);
    if (beginRecord != null && record instanceof OperationLogRecord) {
      @SuppressWarnings("unchecked")
      OperationLogRecord<HKey> oplr = (OperationLogRecord<HKey>) record;
      byte[] row = oplr.getKey().getRow();
      row = HRegionTransactionManager.getSplitRow(region.getConf(),
          region.getTableDesc(), row);
      rows.put(tid, row);
      Put beginPut = newPut(beginRecord);
      puts.add(beginPut);
    }
    puts.add(newPut(record));
    logTable.put(puts);

    if (isFinalized(record)) {
      begins.remove(tid);
      rows.remove(tid);
    }
    return record.getSID();
  }

  protected boolean isStarted(HLogRecord record) {
    return record.getType() == Log.RECORD_TYPE_STATE_TRANSITION
        && ((StateTransitionLogRecord) record).getTransactionState() == TransactionState.STARTED;
  }

  protected boolean isFinalized(HLogRecord record) {
    return record.getType() == Log.RECORD_TYPE_STATE_TRANSITION
        && ((StateTransitionLogRecord) record).getTransactionState() == TransactionState.FINALIZED;
  }

  /*
   * any better way to do this?? It seems there is no 'delete' scanner API.
   */
  @Override
  public void truncate(long sid) throws IOException {
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

  private Put newPut(HLogRecord record) throws IOException {
    byte[] row = rows.get(record.getTID());
    long timestamp = record.getSID();
    byte[] qualifier = regionInfo.getTableName();
    DataOutputBuffer out = new DataOutputBuffer();
    out.writeInt(record.getType());
    record.write(out);
    out.close();
    byte[] value = out.getData();
    Put put = new Put(row, timestamp);
    put.add(family, qualifier, value);
    return put;
  }

  // TODO consider overflow
  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }
}
