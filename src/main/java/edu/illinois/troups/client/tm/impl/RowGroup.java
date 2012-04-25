package edu.illinois.troups.client.tm.impl;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

class RowGroup {

  private final HTable table;
  // table name assumed to stay constant for lifetime of instance
  private final byte[] tableName;
  private final byte[] rootRow;
  // cache for efficient hashCode() and equals()
  private Integer hashCode;

  public RowGroup(HTable table, byte[] rootRow) {
    this.table = table;
    this.tableName = table.getTableName();
    this.rootRow = rootRow;
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
      if (hashCode() != o.hashCode())
        return false;
      return Bytes.equals(o.tableName, tableName)
          && Bytes.equals(o.rootRow, rootRow);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (hashCode == null)
      hashCode = Bytes.hashCode(tableName) ^ Bytes.hashCode(rootRow);
    return hashCode;
  }

}