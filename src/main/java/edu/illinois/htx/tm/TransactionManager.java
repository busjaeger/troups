package edu.illinois.htx.tm;

import java.io.IOException;

public interface TransactionManager {

  public static final long VERSION = 1L;

  /**
   * Begins a new local transaction with the given transaction ID
   * 
   * @param tid
   * @throws IllegalStateException
   *           if a transaction with the given ID already exists
   * @throws IllegalArgumentException
   *           if the given transaction ID is invalid
   */
  void begin(long tid) throws IOException;

  int enlist(long tid) throws IOException;

  void commit(long tid) throws TransactionAbortedException, IOException;

  void abort(long tid) throws IOException;

}