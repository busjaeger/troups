package edu.illinois.troups.client.tm.impl;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troups.tmg.impl.HKey;

class RowGroup {

  private final HTable table;
  // table name assumed to stay constant for lifetime of instance
  private final byte[] tableName;
  private final HKey groupKey;
  // cache for efficient hashCode() and equals()
  private Integer hashCode;

  public RowGroup(HTable table, byte[] row) {
    this(table, table.getTableName(), new HKey(row));
  }

  public RowGroup(HTable table, byte[] tableName, HKey groupKey) {
    this.table = table;
    this.tableName = tableName;
    this.groupKey = groupKey;
  }

  public HTable getTable() {
    return table;
  }

  public byte[] getTableName() {
    return tableName;
  }

  public HKey getKey() {
    return groupKey;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RowGroup) {
      RowGroup o = (RowGroup) obj;
      if (hashCode() != o.hashCode())
        return false;
      return Bytes.equals(o.tableName, tableName)
          && groupKey.equals(o.groupKey);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (hashCode == null)
      hashCode = Bytes.hashCode(tableName) ^ groupKey.hashCode();
    return hashCode;
  }

}