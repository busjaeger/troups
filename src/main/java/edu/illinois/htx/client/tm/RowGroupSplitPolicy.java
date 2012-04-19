package edu.illinois.htx.client.tm;

public interface RowGroupSplitPolicy {

  byte[] getSplitRow(byte[] row);

}