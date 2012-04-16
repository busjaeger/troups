package edu.illinois.htx.client.impl;

import org.apache.hadoop.hbase.util.Bytes;

class RowGroup {

  private final byte[] tableName;
  private final byte[] rootRow;
  private final int hashCode;

  public RowGroup(byte[] tableName, byte[] rootRow) {
    this.tableName = tableName;
    this.rootRow = rootRow;
    this.hashCode = Bytes.hashCode(tableName) ^ Bytes.hashCode(rootRow);
  }

  public byte[] getTableName() {
    return tableName;
  }

  public byte[] getRootRow() {
    return rootRow;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RowGroup) {
      RowGroup o = (RowGroup) obj;
      return Bytes.equals(o.tableName, tableName)
          && Bytes.equals(o.rootRow, rootRow);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}