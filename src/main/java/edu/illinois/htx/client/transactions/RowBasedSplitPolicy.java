package edu.illinois.htx.client.transactions;

public interface RowBasedSplitPolicy {

  byte[] getRootRow(byte[] row);

}
