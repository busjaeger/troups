package edu.illinois.troups.tm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

public class TID implements Writable {

  private long ts;

  // for serialization only
  public TID() {
    super();
  }

  public TID(long ts) {
    this.ts = ts;
  }

  public TID(byte[] bytes) {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    try {
      readFields(in);
      in.close();
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public long getTS() {
    return ts;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TID ? ts == ((TID) obj).ts : false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(ts).hashCode();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.ts);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.ts = in.readLong();
  }

  @Override
  public String toString() {
    return String.valueOf(ts);
  }
}
