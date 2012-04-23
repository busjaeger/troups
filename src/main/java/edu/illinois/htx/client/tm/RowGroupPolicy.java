package edu.illinois.htx.client.tm;

public interface RowGroupPolicy {

  public static final String META_ATTR = "ROW_GROUP_POLICY";

  byte[] getGroupRow(byte[] row);

}