package edu.illinois.troups.test;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog;

public class StringKeyLog implements
    TransactionLog<StringKey, StringKeyLogRecord> {

  @Override
  public int compare(Long o1, Long o2) {
    return 0;
  }

  @Override
  public long appendStateTransition(TID tid, int state) throws IOException {
    return 0;
  }

  @Override
  public void truncate(long sid) throws IOException {
  }

  @Override
  public NavigableMap<Long, StringKeyLogRecord> open() throws IOException {
    return new TreeMap<Long, StringKeyLogRecord>();
  }

  @Override
  public long appendGet(TID tid, List<StringKey> keys, List<Long> version)
      throws IOException {
    return 0;
  }

  @Override
  public long appendPut(TID tid, List<StringKey> keys) throws IOException {
    return 0;
  }

}
