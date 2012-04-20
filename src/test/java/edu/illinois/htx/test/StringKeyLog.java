package edu.illinois.htx.test;

import java.io.IOException;
import java.util.Collections;

import edu.illinois.htx.tm.log.Log;

public class StringKeyLog implements Log<StringKey, StringKeyLogRecord> {

  @Override
  public long appendStateTransition(long tid, int state) throws IOException {
    return 0;
  }

  @Override
  public long appendGet(long tid, StringKey key, long version)
      throws IOException {
    return 0;
  }

  @Override
  public long appendPut(long tid, StringKey key) throws IOException {
    return 0;
  }

  @Override
  public long appendDelete(long tid, StringKey key) throws IOException {
    return 0;
  }

  @Override
  public void savepoint(long sid) throws IOException {
  }

  @Override
  public Iterable<StringKeyLogRecord> recover() throws IOException {
    return Collections.emptyList();
  }

}
