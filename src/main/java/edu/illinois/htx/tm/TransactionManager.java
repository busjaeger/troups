package edu.illinois.htx.tm;

import java.io.IOException;

public interface TransactionManager {

  public static final long VERSION = 1L;

  long begin() throws IOException;

  void commit(long tid) throws TransactionAbortedException, IOException;

  void abort(long tid) throws IOException;

}
