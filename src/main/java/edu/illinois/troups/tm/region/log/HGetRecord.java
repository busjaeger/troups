package edu.illinois.troups.tm.region.log;

import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_GET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog.GetRecord;
import edu.illinois.troups.tm.region.HKey;

class HGetRecord extends HOperationRecord implements GetRecord<HKey> {

  private List<Long> versions;

  HGetRecord() {
    super(RECORD_TYPE_GET);
  }

  public HGetRecord(TID tid, List<HKey> keys, List<Long> version) {
    super(RECORD_TYPE_GET, tid, keys);
    this.versions = version;
  }

  @Override
  public List<Long> getVersions() {
    return versions;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int size = in.readInt();
    versions = new ArrayList<Long>(size);
    for (int i = 0; i < size; i++)
      versions.add(in.readLong());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(versions.size());
    for (Long version : versions)
      out.writeLong(version);
  }

}
