package edu.illinois.htx.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.htx.tm.log.OperationLogRecord;

public abstract class HOperationLogRecord extends HLogRecord implements
    OperationLogRecord<HKey> {

  private HKey key;

  public HOperationLogRecord(int type) {
    super(type);
  }

  public HOperationLogRecord(int type, long sid, long tid, HKey key) {
    super(type, sid, tid);
    this.key = key;
  }

  @Override
  public HKey getKey() {
    return key;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    key = new HKey();
    key.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    key.write(out);
  }

}
