package edu.illinois.htx.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Function;

import edu.illinois.htx.tm.Key;

public class HKey implements Key, Writable {

  static final Function<KeyValue, HKey> KEYVALUE_TO_KEY = new Function<KeyValue, HKey>() {
    @Override
    public HKey apply(KeyValue kv) {
      return new HKey(kv);
    }
  };

  protected byte[] row;
  protected byte[] family;
  protected byte[] qualifier;

  protected Integer hash;

  public HKey() {
    super();
  }

  public HKey(byte[] row) {
    this.row = row;
  }

  public HKey(KeyValue keyValue) {
    this(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier());
  }

  public HKey(byte[] row, byte[] family, byte[] qualifier) {
    assert row != null;
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
  }

  public HKey(HKey key) {
    this.row = key.row;
    this.family = key.family;
    this.qualifier = key.qualifier;
  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HKey) {
      HKey k = (HKey) obj;
      if (!Arrays.equals(row, k.row))
        return false;
      if (!equalOrNull(family, k.family))
        return false;
      return equalOrNull(qualifier, k.qualifier);
    }
    return false;
  }

  private boolean equalOrNull(byte[] first, byte[] second) {
    if (first == null && second == null)
      return true;
    if (first == null && second != null)
      return false;
    if (second == null && first != null)
      return false;
    return Arrays.equals(first, second);
  }

  @Override
  public int hashCode() {
    if (hash == null) {
      hash = Arrays.hashCode(row);
      if (family != null)
        hash *= Arrays.hashCode(family);
      if (qualifier != null)
        hash *= Arrays.hashCode(qualifier);
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
    if (b == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(b.length);
      out.write(b);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.row = readByteArray(in);
    this.family = readByteArray(in);
    this.qualifier = readByteArray(in);
  }

  private static byte[] readByteArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1)
      return null;
    byte[] b = new byte[len];
    in.readFully(b);
    return b;
  }

}
