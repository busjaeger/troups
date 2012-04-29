package edu.illinois.troups.tm.region.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog.OperationRecord;
import edu.illinois.troups.tm.region.HKey;

public abstract class HOperationRecord extends HRecord implements
    OperationRecord<HKey> {

  private HKey key;

  public HOperationRecord(int type) {
    super(type);
  }

  public HOperationRecord(int type, TID tid, HKey key) {
    super(type, tid);
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
