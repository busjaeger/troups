package edu.illinois.htx.tm;

public interface TransactionManager {

  public static final long VERSION = 1L;

  void commit(long tid) throws TransactionAbortedException;

  void abort(long tid);

}