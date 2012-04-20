package edu.illinois.htx.tm;

import java.io.IOException;

public interface TransactionManager {

  public static final long VERSION = 1L;

  TID begin() throws IOException;

  void commit(TID tid) throws TransactionAbortedException, IOException;

  void abort(TID tid) throws IOException;

}