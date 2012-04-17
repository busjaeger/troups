package edu.illinois.htx.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.illinois.htx.tm.LogRecord;

public class HLogRecord implements LogRecord<HKey>, Writable, Comparable<HLogRecord> {

  private static final int VERSION = 0;

  private long sid;
  private long tid;
  private Type type;
  private HKey key;
  private Long version;

  public HLogRecord() {
    super();
  }

  public HLogRecord(long sid, long tid, Type type, HKey key, Long version) {
    this.sid = sid;
    this.tid = tid;
    this.type = type;
    this.key = key;
    this.version = version;
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
  public Type getType() {
    return type;
  }

  @Override
  public HKey getKey() {
    return key;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public int compareTo(HLogRecord o) {
    return Long.valueOf(sid).compareTo(o.sid);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeLong(sid);
    out.writeLong(tid);
    out.writeInt(type.ordinal());
    key.write(out);
    boolean hasVersion = version != null;
    out.writeBoolean(hasVersion);
    if (hasVersion)
      out.writeLong(version);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readInt();
    sid = in.readLong();
    tid = in.readLong();
    type = Type.values()[in.readInt()];
    key = new HKey();
    key.readFields(in);
    if (in.readBoolean())
      version = in.readLong();
  }

}
