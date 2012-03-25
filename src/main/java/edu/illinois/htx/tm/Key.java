package edu.illinois.htx.tm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Writable;

/**
 * Could be made abstract to decouple from HBase row/family/qualifier triple
 */
public class Key implements Writable {

  protected byte[] row;
  protected byte[] family;
  protected byte[] qualifier;

  protected Integer hash;

  public Key() {
    super();
  }

  public Key(KeyValue keyValue) {
    this(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier());
  }

  public Key(byte[] row, byte[] family, byte[] qualifier) {
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
  }

  public Key(Key key) {
    this.row = key.row;
    this.family = key.family;
    this.qualifier = key.qualifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Key) {
      Key k = (Key) obj;
      return Arrays.equals(row, k.row) && Arrays.equals(family, k.family)
          && Arrays.equals(qualifier, k.qualifier);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (hash == null) {
      hash = Arrays.hashCode(row) * Arrays.hashCode(family)
          * Arrays.hashCode(qualifier);
    }
    return hash;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeByteArray(row, out);
    writeByteArray(family, out);
    writeByteArray(qualifier, out);
  }

  private static void writeByteArray(byte[] b, DataOutput out)
      throws IOException {
    out.writeInt(b.length);
    out.write(b);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.row = readByteArray(in);
    this.family = readByteArray(in);
    this.qualifier = readByteArray(in);
  }

  private static byte[] readByteArray(DataInput in) throws IOException {
    int len = in.readInt();
    byte[] b = new byte[len];
    in.readFully(b);
    return b;
  }
}
