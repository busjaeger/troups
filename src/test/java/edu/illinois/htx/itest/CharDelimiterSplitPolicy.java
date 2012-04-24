package edu.illinois.htx.itest;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.client.tm.RowGroupPolicy;

public class CharDelimiterSplitPolicy implements RowGroupPolicy {

  private final char delimiter;

  public CharDelimiterSplitPolicy(HTableDescriptor descr) {
    String delim = descr.getValue("SPLIT_DELIMINTER");
    this.delimiter = delim == null ? '.' : delim.charAt(0);
  }

  @Override
  public byte[] getGroupKey(byte[] row) {
    String s = Bytes.toString(row);
    int idx = s.indexOf(delimiter);
    if (idx != -1)
      row = Bytes.toBytes(s.substring(0, idx));
    return row;
  }

}
