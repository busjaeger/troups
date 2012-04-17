package edu.illinois.htx.tm;

import java.io.IOException;

public interface XATransactionManager {

  public static final long VERSION = 1L;

  long join(long tid) throws IOException;

  void prepare(long tid) throws TransactionAbortedException, IOException;

  void commit(long tid) throws IOException;

  void abort(long tid) throws IOException;

}