package edu.illinois.htx.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.log.GetLogRecord;
import edu.illinois.htx.tm.log.Log;

class HGetLogRecord extends HOperationLogRecord implements GetLogRecord<HKey> {

  private long version;

  HGetLogRecord() {
    super(Log.RECORD_TYPE_GET);
  }

  public HGetLogRecord(long sid, TID tid, HKey groupKey, HKey key, long version) {
    super(Log.RECORD_TYPE_GET, sid, tid, groupKey, key);
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
