package edu.illinois.htx.tm;

public interface TransactionManager {

  /**
   * 
   * @param tid
   *          must be higher than any previous tid passed to this invocation
   */
  void begin(long tid);

  void commit(long tid) throws TransactionAbortedException;

  void abort(long tid);

}