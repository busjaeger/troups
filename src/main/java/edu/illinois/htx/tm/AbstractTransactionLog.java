package edu.illinois.htx.tm;

import edu.illinois.htx.tm.TransactionLog.Record.Type;

public abstract class AbstractTransactionLog<K extends Key> implements
    TransactionLog<K> {

  @Override
  public Record<K> newRecord(Type type, long tid) {
    return newRecord(type, tid, null);
  }

  public Record<K> newRecord(Type type, long tid, K key) {
    return newRecord(type, tid, key, -1);
  }

  @Override
  public long appendBegin(long tid) {
    return append(newRecord(Type.BEGIN, tid));
  }

  @Override
  public long appendCommit(long tid) {
    return append(newRecord(Type.COMMIT, tid));
  }

  @Override
  public long appendAbort(long tid) {
    return append(newRecord(Type.ABORT, tid));
  }

  @Override
  public long appendFinalize(long tid) {
    return append(newRecord(Type.FINALIZE, tid));
  }

  @Override
  public long appendRead(long tid, K key, long version) {
    return append(newRecord(Type.READ, tid, key, version));
  }

  @Override
  public long appendWrite(long tid, K key, boolean isDelete) {
    return append(newRecord(isDelete ? Type.DELETE : Type.WRITE, tid, key));
  }

}
