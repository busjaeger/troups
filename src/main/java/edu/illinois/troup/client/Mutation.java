package edu.illinois.troup.client;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class Mutation {

  protected final byte[] row;
  protected final Map<byte[], List<KeyValue>> familyMap = new TreeMap<byte[], List<KeyValue>>(
      Bytes.BYTES_COMPARATOR);

  protected Mutation(byte[] row) {
    this.row = row;
  }

  public byte[] getRow() {
    return row;
  }

  public Map<byte[], List<KeyValue>> getFamilyMap() {
    return familyMap;
  }

}
