package edu.illinois.troups.client;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Like HBase's Put, but restricted to operations troups supports. In
 * particular, no time-stamp support.
 */
public class Get {

  private final byte[] row;
  private final Map<byte[], NavigableSet<byte[]>> familyMap;

  public Get(byte[] row) {
    this.row = row;
    this.familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
        Bytes.BYTES_COMPARATOR);
  }

  public byte[] getRow() {
    return this.row;
  }

  public Get addColumn(byte[] family, byte[] qualifier) {
    NavigableSet<byte[]> set = familyMap.get(family);
    if (set == null) {
      set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    }
    set.add(qualifier);
    familyMap.put(family, set);
    return this;
  }

  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return this.familyMap;
  }

}
