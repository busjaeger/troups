package edu.illinois.troups.tmg.impl;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog;

public class HGroupTransactionLog implements TransactionLog<HKey, HRecord> {

  protected final HKey groupKey;
  protected final GroupLogStore logStore;
  private final AtomicLong sidCounter = new AtomicLong();

  public HGroupTransactionLog(HKey groupKey, GroupLogStore logStore) {
    this.groupKey = groupKey;
    this.logStore = logStore;
  }

  @Override
  public NavigableMap<Long, HRecord> open() throws IOException {
    NavigableMap<Long, byte[]> versions = logStore.open(groupKey);
    NavigableMap<Long, HRecord> records = new TreeMap<Long, HRecord>();
    long highestSID = versions.isEmpty() ? 0L : versions.firstKey();
    for (Entry<Long, byte[]> version : versions.entrySet()) {
      long sid = version.getKey();
      if (sid > highestSID)
        highestSID = sid;
      byte[] cell = version.getValue();
      DataInputBuffer in = new DataInputBuffer();
      in.reset(cell, cell.length);
      int type = in.readInt();
      HRecord record = create(type);
      record.readFields(in);
      in.close();
      records.put(sid, record);
    }
    sidCounter.set(highestSID);
    return records;
  }

  @Override
  public long appendStateTransition(TID tid, int state) throws IOException {
    HRecord record = new HStateTransitionRecord(tid, state);
    return append(groupKey, record);
  }

  @Override
  public long appendGet(TID tid, HKey key, long version) throws IOException {
    HRecord record = new HGetRecord(tid, key, version);
    return append(groupKey, record);
  }

  @Override
  public long appendPut(TID tid, HKey key) throws IOException {
    HRecord record = new HPutRecord(tid, key);
    return append(groupKey, record);
  }

  @Override
  public long appendDelete(TID tid, HKey key) throws IOException {
    HRecord record = new HDeleteRecord(tid, key);
    return append(groupKey, record);
  }

  @Override
  public void truncate(long sid) throws IOException {
    logStore.truncate(groupKey, sid);
  }

  @Override
  public int compare(Long o1, Long o2) {
    // TODO handle overflow
    return o1.compareTo(o2);
  }

  protected long append(HKey groupKey, HRecord record) throws IOException {
    // 1. serialize the log record - note: the group key is not serialized,
    // since it's already used as the log table row key
    DataOutputBuffer out = new DataOutputBuffer();
    // write type so we know which class to create during recovery
    out.writeInt(record.getType());
    record.write(out);
    out.close();
    byte[] value = out.getData();

    // 2. store in log
    long sid = sidCounter.getAndIncrement();
    logStore.append(groupKey, sid, value);

    // 3. return log record sequence ID
    return sid;
  }

  protected HRecord create(int type) {
    switch (type) {
    case RECORD_TYPE_STATE_TRANSITION:
      return new HStateTransitionRecord();
    case RECORD_TYPE_GET:
      return new HGetRecord();
    case RECORD_TYPE_DELETE:
      return new HDeleteRecord();
    case RECORD_TYPE_PUT:
      return new HPutRecord();
    default:
      throw new IllegalStateException("Unknown log record type: " + type);
    }
  }

}