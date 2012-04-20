package edu.illinois.htx.tm;

import java.io.IOException;

public interface TransactionOperationObserver<K extends Key> {

  void beforeGet(TID tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedGet(TID tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterGet(TID tid, Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException;

  void beforePut(TID tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedPut(TID tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterPut(TID tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void beforeDelete(TID tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedDelete(TID tid, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterDelete(TID tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

}