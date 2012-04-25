package edu.illinois.troups.client.tm;

public interface RowGroupPolicy {

  public static final String META_ATTR = "ROW_GROUP_POLICY";

  byte[] getGroupKey(byte[] row);

}