package edu.illinois.htx.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.illinois.htx.tm.log.LogRecord;

public abstract class HLogRecord implements LogRecord, Writable,
    Comparable<HLogRecord> {

  private transient final int type;
  private long sid;
  private long tid;

  HLogRecord(int type) {
    this.type = type;
  }

  HLogRecord(int type, long sid, long tid) {
    this(type);
    this.sid = sid;
    this.tid = tid;
  }

  @Override
  public long getSID() {
    return sid;
  }

  @Override
  public long getTID() {
    return tid;
  }

  @Override
  public int getType() {
    return type;
  }

  @Override
  public int compareTo(HLogRecord o) {
    return Long.valueOf(sid).compareTo(o.sid);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(sid);
    out.writeLong(tid);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sid = in.readLong();
    tid = in.readLong();
  }

}
