package edu.illinois.troups.client;

public class KeyValue {

  private final byte[] row;
  private final byte[] family;
  private final byte[] qualifier;
  private final byte[] value;

  public KeyValue(byte[] row, byte[] family, byte[] qualifier) {
    this(row, family, qualifier, null);
  }

  public KeyValue(byte[] row, byte[] family, byte[] qualifier, byte[] value) {
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
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

  public byte[] getValue() {
    return value;
  }
}
