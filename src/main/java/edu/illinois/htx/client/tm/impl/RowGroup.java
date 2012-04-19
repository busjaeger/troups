package edu.illinois.htx.client.tm.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

class RowGroup {

  private final HTable table;
  // table name assumed to stay constant for lifetime of instance
  private final byte[] tableName;
  private final byte[] rootRow;
  // pre-computed for efficient hashCode() and equals()
  private final int hashCode;

  public RowGroup(HTable table, byte[] rootRow) throws IOException {
    this(table, table.getTableDescriptor().getName(), rootRow);
  }

  public RowGroup(HTable table, byte[] tableName, byte[] rootRow) {
    this.table = table;
    this.tableName = tableName;
    this.rootRow = rootRow;
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
      if (hashCode != o.hashCode)
        return false;
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