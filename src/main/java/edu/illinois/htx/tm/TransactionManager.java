package edu.illinois.htx.tm;

public interface TransactionManager {

  public static final long VERSION = 1L;

  /**
   * Begins a new transaction with the given transaction ID
   * 
   * @param tid
   * @throws IllegalStateException
   *           if a transaction with the given ID already exists
   * @throws IllegalArgumentException
   *           if the given transaction ID is invalid
   */
  void begin(long tid);

  void commit(long tid) throws TransactionAbortedException;

  void abort(long tid);

}