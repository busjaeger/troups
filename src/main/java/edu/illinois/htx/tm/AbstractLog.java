package edu.illinois.htx.tm;

import java.io.IOException;

import edu.illinois.htx.tm.LogRecord.Type;

public abstract class AbstractLog<K extends Key, R extends LogRecord<K>>
    implements Log<K, R> {

  @Override
  public long appendBegin(long tid) throws IOException {
    return appendRecord(newRecord(Type.BEGIN, tid));
  }

  @Override
  public long appendCommit(long tid) throws IOException {
    return appendRecord(newRecord(Type.COMMIT, tid));
  }

  @Override
  public long appendAbort(long tid) throws IOException {
    return appendRecord(newRecord(Type.ABORT, tid));
  }

  @Override
  public long appendFinalize(long tid) throws IOException {
    return appendRecord(newRecord(Type.FINALIZE, tid));
  }

  @Override
  public long appendRead(long tid, K key, long version) throws IOException {
    return appendRecord(newRecord(Type.READ, tid, key, version));
  }

  @Override
  public long appendWrite(long tid, K key, boolean isDelete) throws IOException {
    return appendRecord(newRecord(isDelete ? Type.DELETE : Type.WRITE, tid, key));
  }

  protected R newRecord(Type type, long tid) {
    return newRecord(type, tid, null);
  }

  protected R newRecord(Type type, long tid, K key) {
    return newRecord(type, tid, key, -1);
  }

  protected long appendRecord(R record) throws IOException {
    append(record);
    return record.getSID();
  }

}
