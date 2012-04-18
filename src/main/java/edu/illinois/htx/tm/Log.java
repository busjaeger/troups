package edu.illinois.htx.tm;

import java.io.IOException;

import edu.illinois.htx.tm.LogRecord.Type;

public abstract class Log<K extends Key, R extends LogRecord<K>> {

  public abstract R newRecord(Type type, long tid, K key, Long version, Long pid);

  public abstract void append(R record) throws IOException;

  public abstract void savepoint(long sid) throws IOException;

  public abstract Iterable<R> recover() throws IOException;

  // convenience methods

  public long append(Type type, long tid) throws IOException {
    return append(type, tid, (K)null);
  }

  public long append(Type type, long tid, Long pid) throws IOException {
    return append(type, tid, null, null, pid);
  }

  public long append(Type type, long tid, K key) throws IOException {
    return append(type, tid, key, null);
  }

  public long append(Type type, long tid, K key, Long version) throws IOException {
    return append(type, tid, key, version, null);
  }

  public long append(Type type, long tid, K key, Long version, Long pid)
      throws IOException {
    R record = newRecord(type, tid, key, version, pid);
    append(record);
    return record.getSID();
  }

}