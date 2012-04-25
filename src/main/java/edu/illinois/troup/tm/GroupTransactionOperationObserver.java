package edu.illinois.troup.tm;

import java.io.IOException;

public interface GroupTransactionOperationObserver<K extends Key> {

  void beforeGet(TID tid, K groupKey, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedGet(TID tid, K groupKey, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterGet(TID tid, K groupKey, Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException;

  void beforePut(TID tid, K groupKey, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedPut(TID tid, K groupKey, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterPut(TID tid, K groupKey, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void beforeDelete(TID tid, K groupKey, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void failedDelete(TID tid, K groupKey, Iterable<? extends K> keys, Throwable t)
      throws TransactionAbortedException, IOException;

  void afterDelete(TID tid, K groupKey, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

}