package edu.illinois.htx.tm;

import edu.illinois.htx.tm.TransactionLog.Entry.Type;

public abstract class AbstractTransactionLog<K extends Key> implements
    TransactionLog<K> {

  @Override
  public Entry<K> newEntry(Type type, long tid) {
    return newEntry(type, tid, null);
  }

  public Entry<K> newEntry(Type type, long tid, K key) {
    return newEntry(type, tid, key, -1);
  }

  @Override
  public void appendBegin(long tid) {
    append(newEntry(Type.BEGIN, tid));
  }

  @Override
  public void appendCommit(long tid) {
    append(newEntry(Type.COMMIT, tid));
  }

  @Override
  public void appendAbort(long tid) {
    append(newEntry(Type.ABORT, tid));
  }

  @Override
  public void appendFinalize(long tid) {
    append(newEntry(Type.FINALIZE, tid));
  }

  @Override
  public void appendRead(long tid, K key, long version) {
    append(newEntry(Type.READ, tid, key, version));
  }

  @Override
  public void appendWrite(long tid, K key, boolean isDelete) {
    append(newEntry(isDelete ? Type.DELETE : Type.WRITE, tid, key));
  }

}
