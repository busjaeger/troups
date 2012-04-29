package edu.illinois.troups.tm.region.log;

import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_GET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog.GetRecord;
import edu.illinois.troups.tm.region.HKey;

class HGetRecord extends HOperationRecord implements GetRecord<HKey> {

  private long version;

  HGetRecord() {
    super(RECORD_TYPE_GET);
  }

  public HGetRecord(TID tid, HKey key, long version) {
    super(RECORD_TYPE_GET, tid, key);
    this.version = version;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    version = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(version);
  }

}
