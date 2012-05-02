package edu.illinois.troups.tm.region.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog.OperationRecord;
import edu.illinois.troups.tm.region.HKey;

public abstract class HOperationRecord extends HRecord implements
    OperationRecord<HKey> {

  private List<HKey> keys;

  public HOperationRecord(int type) {
    super(type);
  }

  public HOperationRecord(int type, TID tid, List<HKey> key) {
    super(type, tid);
    this.keys = key;
  }

  @Override
  public List<HKey> getKeys() {
    return keys;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int size = in.readInt();
    keys = new ArrayList<HKey>(size);
    for (int i = 0; i < size; i++) {
      HKey key = new HKey();
      key.readFields(in);
      keys.add(key);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(keys.size());
    for (HKey key : keys)
      key.write(out);
  }

}
