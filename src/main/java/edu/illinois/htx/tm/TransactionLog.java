package edu.illinois.htx.tm;

import edu.illinois.htx.tm.TransactionLog.Record.Type;

public interface TransactionLog<K extends Key> {

  public interface Record<K> {

    public enum Type {
      BEGIN, READ, WRITE, DELETE, COMMIT, ABORT, FINALIZE
    }

    Type getType();

    /**
     * log sequence number
     * 
     * @return
     */
    long getSID();

    /**
     * transaction ID
     * 
     * @return
     */
    long getTID();

    K getKey();

    long getVersion();
  }

  Record<K> newRecord(Type type, long tid);

  Record<K> newRecord(Type type, long tid, K key);

  Record<K> newRecord(Type type, long tid, K key, long version);

  long append(Record<K> record);

  long appendBegin(long tid);

  long appendCommit(long tid);

  long appendAbort(long tid);

  long appendFinalize(long tid);

  long appendRead(long tid, K key, long version);

  long appendWrite(long tid, K key, boolean isDelete);

  Iterable<Record<K>> read();

  void savepoint(long sid);
}