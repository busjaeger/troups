package edu.illinois.htx.tm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

public class XID extends TID implements Writable {

  private long pid;

  // for serialization only
  public XID() {
    super();
  }

  public XID(long ts, long pid) {
    super(ts);
    this.pid = pid;
  }

  public XID(byte[] bytes) {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    try {
      readFields(in);
      in.close();
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public long getPid() {
    return pid;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj))
      return false;
    return obj instanceof XID ? ((XID) obj).pid == pid : false;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ Long.valueOf(pid).hashCode();
  }

  @Override
  public int compareTo(TID o) {
    int c = super.compareTo(o);
    if (c == 0 && o instanceof XID) {
      XID xid = (XID) o;
      c = Long.valueOf(pid).compareTo(xid.pid);
    }
    return c;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(pid);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    pid = in.readLong();
  }

  @Override
  public String toString() {
    return super.toString() + "-" + pid;
  }
}
