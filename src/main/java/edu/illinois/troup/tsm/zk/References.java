package edu.illinois.troup.tsm.zk;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

class References implements Writable {

  private final Set<Long> references;

  References() {
    this.references = new HashSet<Long>();
  }

  References(byte[] bytes) {
    this();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    try {
      readFields(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  References(Iterable<Long> rids) {
    this();
    for (Long rid : rids)
      references.add(rid);
  }

  public Set<Long> getRIDs() {
    return references;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(references.size());
    for (Long rid : references)
      out.writeLong(rid);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++)
      references.add(in.readLong());
  }

}
