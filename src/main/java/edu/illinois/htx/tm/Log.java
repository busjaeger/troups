package edu.illinois.htx.tm;

import java.io.IOException;

import edu.illinois.htx.tm.LogRecord.Type;

public abstract class Log<K extends Key, R extends LogRecord<K>> {

  public abstract R newRecord(Type type, long tid, K key, Long version);

  public abstract void append(R record) throws IOException;

  public abstract void savepoint(long sid) throws IOException;

  public abstract Iterable<R> recover() throws IOException;

  // convenience methods

  public long append(Type type, long tid) throws IOException {
    return append(type, tid, null);
  }

  public long append(Type type, long tid, K key) throws IOException {
    return append(type, tid, key, null);
  }

  public long append(Type type, long tid, K key, Long version)
      throws IOException {
    R record = newRecord(type, tid, key, version);
    append(record);
    return record.getSID();
  }

}