package edu.illinois.htx.tm.log;

import java.io.IOException;

import edu.illinois.htx.tm.Key;

public interface Log<K extends Key, R extends LogRecord> {

  public static final int RECORD_TYPE_STATE_TRANSITION = 1;
  public static final int RECORD_TYPE_GET = 2;
  public static final int RECORD_TYPE_PUT = 3;
  public static final int RECORD_TYPE_DELETE = 4;

  public long appendStateTransition(long tid, int state) throws IOException;

  public long appendGet(long tid, K key, long version) throws IOException;

  public long appendPut(long tid, K key) throws IOException;

  public long appendDelete(long tid, K key) throws IOException;

  public abstract void savepoint(long sid) throws IOException;

  public abstract Iterable<R> recover() throws IOException;

}