package edu.illinois.htx.tm;

import java.io.IOException;

public interface TransactionOperationObserver<K extends Key> {

  void beforeGet(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedGet(long tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterGet(long tid, Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException;

  void beforePut(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedPut(long tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterPut(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void beforeDelete(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedDelete(long tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterDelete(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

}