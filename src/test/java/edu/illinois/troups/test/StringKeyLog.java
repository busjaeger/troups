package edu.illinois.troups.test;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog;

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
  public long appendGet(TID tid, StringKey key, long version)
      throws IOException {
    return 0;
  }

  @Override
  public long appendPut(TID tid, StringKey key) throws IOException {
    return 0;
  }

  @Override
  public long appendDelete(TID tid, StringKey key) throws IOException {
    return 0;
  }

  @Override
  public void truncate(long sid) throws IOException {
  }

  @Override
  public NavigableMap<Long, StringKeyLogRecord> open() throws IOException {
    return new TreeMap<Long, StringKeyLogRecord>();
  }

}
