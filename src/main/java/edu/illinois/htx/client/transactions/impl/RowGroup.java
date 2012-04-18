package edu.illinois.htx.client.transactions.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

class RowGroup {

  private final HTable table;
  private final byte[] rootRow;

  // pre-computed
  private final byte[] tableName;
  private final int hashCode;

  public RowGroup(HTable table, byte[] rootRow) throws IOException {
    this.table = table;
    this.rootRow = rootRow;
    this.tableName = table.getTableDescriptor().getName();
    this.hashCode = Bytes.hashCode(tableName) ^ Bytes.hashCode(rootRow);
  }

  public HTable getTable() {
    return table;
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