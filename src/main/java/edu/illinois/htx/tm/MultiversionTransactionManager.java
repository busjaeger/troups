package edu.illinois.htx.tm;

import java.io.IOException;



public interface MultiversionTransactionManager<K extends Key> extends
    TransactionManager {

  void filterReads(long tid, Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException;

  void preWrite(long tid, boolean isDelete, Iterable<? extends K> key)
      throws TransactionAbortedException, IOException;

  void postWrite(long tid, boolean isDelete, Iterable<? extends K> key)
      throws TransactionAbortedException, IOException;

  long getFirstActiveTID();

}