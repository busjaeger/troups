package edu.illinois.troups.tmg.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog.Record;

public abstract class HRecord implements Record<HKey>, Writable {

  // transient as in these won't be written into the DB log record
  private transient final int type;
  private TID tid;

  HRecord(int type) {
    this.type = type;
  }

  HRecord(int type, TID tid) {
    this(type);
    this.tid = tid;
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
  public void write(DataOutput out) throws IOException {
    tid.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tid = createTID();
    tid.readFields(in);
  }

  protected TID createTID() {
    return new TID();
  }

}
