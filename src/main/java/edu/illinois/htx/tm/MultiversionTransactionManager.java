package edu.illinois.htx.tm;


public interface MultiversionTransactionManager<K extends Key> extends
    TransactionManager {

  void filterReads(long tid, Iterable<? extends KeyVersion<K>> versions);

  void checkWrite(long tid, K key) throws TransactionAbortedException;

  void checkDelete(long tid, K key) throws TransactionAbortedException;

}