package edu.illinois.htx.tm;

import java.io.IOException;

public interface KeyValueStoreObserver<K extends Key> {

  void beforeRead(long tid, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void afterRead(long tid, Iterable<? extends KeyVersions<K>> kvs)
      throws TransactionAbortedException, IOException;

  void beforeWrite(long tid, boolean isDelete, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

  void afterWrite(long tid, boolean isDelete, Iterable<? extends K> keys)
      throws TransactionAbortedException, IOException;

}