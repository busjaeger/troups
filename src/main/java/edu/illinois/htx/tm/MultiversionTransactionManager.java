package edu.illinois.htx.tm;


public interface MultiversionTransactionManager<K extends Key> extends
    TransactionManager {

  void filterReads(long tid, Iterable<? extends KeyVersion<K>> versions);

  void checkWrite(long tid, K key, boolean isDelete) throws TransactionAbortedException;

  long getFirstActiveTID();

}