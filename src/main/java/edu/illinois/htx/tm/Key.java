package edu.illinois.htx.tm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Writable;

public class Key implements Writable {

  protected byte[] table;
  protected byte[] row;
  protected byte[] family;
  protected byte[] qualifier;

  protected Integer hash;

  public Key() {
    super();
  }

  public Key(byte[] table, KeyValue keyValue) {
    this.table = table;
    this.row = keyValue.getRow();
    this.family = keyValue.getFamily();
    this.qualifier = keyValue.getQualifier();
  }

  public Key(byte[] table, byte[] row, byte[] family, byte[] qualifier) {
    this.table = table;
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
  }

  public Key(Key key) {
    this.table = key.table;
    this.row = key.row;
    this.family = key.family;
    this.qualifier = key.qualifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Key) {
      Key k = (Key) obj;
      return Arrays.equals(table, k.table) && Arrays.equals(row, k.row)
          && Arrays.equals(family, k.family)
          && Arrays.equals(qualifier, k.qualifier);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (hash == null) {
      hash = Arrays.hashCode(table) * Arrays.hashCode(row)
          * Arrays.hashCode(family) * Arrays.hashCode(qualifier);
    }
    return hash;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO
  }

}
