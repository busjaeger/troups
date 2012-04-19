package edu.illinois.htx.tm.log;

import java.io.IOException;

import edu.illinois.htx.tm.Key;

public interface XALog<K extends Key, R extends LogRecord> extends Log<K, R> {

  public static final int RECORD_TYPE_JOIN = 5;

  public abstract long appendJoinLogRecord(long tid, long pid)
      throws IOException;

}
