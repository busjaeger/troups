package edu.illinois.troup.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.log.LogRecord;

public abstract class HLogRecord implements LogRecord<HKey>, Writable,
    Comparable<HLogRecord> {

  // transient as in these won't be written into the DB log record
  private transient final int type;
  private transient HKey groupKey;

  private long sid;
  private TID tid;

  HLogRecord(int type) {
    this.type = type;
  }

  HLogRecord(int type, long sid, TID tid, HKey groupKey) {
    this(type);
    this.sid = sid;
    this.tid = tid;
    this.groupKey = groupKey;
  }

  @Override
  public long getSID() {
    return sid;
  }

  @Override
  public TID getTID() {
    return tid;
  }

  @Override
  public int getType() {
    return type;
  }

  @Override
  public HKey getGroupKey() {
    return groupKey;
  }

  void setGroupKey(HKey groupKey) {
    this.groupKey = groupKey;
  }

  @Override
  public int compareTo(HLogRecord o) {
    return Long.valueOf(sid).compareTo(o.sid);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(sid);
    tid.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sid = in.readLong();
    tid = createTID();
    tid.readFields(in);
  }

  protected TID createTID() {
    return new TID();
  }

}
