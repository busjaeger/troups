package edu.illinois.htx.tm;

import edu.illinois.htx.tm.TransactionLog.Entry.Type;

public interface TransactionLog<K extends Key> {

  public interface Entry<K> {

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

  Entry<K> newEntry(Type type, long tid);

  Entry<K> newEntry(Type type, long tid, K key);

  Entry<K> newEntry(Type type, long tid, K key, long version);

  void append(Entry<K> entry);

  void appendBegin(long tid);

  void appendCommit(long tid);

  void appendAbort(long tid);

  void appendFinalize(long tid);

  void appendRead(long tid, K key, long version);

  void appendWrite(long tid, K key, boolean isDelete);

  Iterable<Entry<K>> read();

}