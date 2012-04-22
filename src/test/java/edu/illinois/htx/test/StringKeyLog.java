package edu.illinois.htx.test;

import java.io.IOException;
import java.util.Collections;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.log.Log;

public class StringKeyLog implements Log<StringKey, StringKeyLogRecord> {

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
  public Iterable<StringKeyLogRecord> recover() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

}
