package edu.illinois.htx.tm;


public interface MultiversionTransactionManager<K extends Key> extends
    TransactionManager {

  void filterReads(long tid, Iterable<? extends KeyVersion<K>> versions) throws TransactionAbortedException;

  void preWrite(long tid, K key, boolean isDelete) throws TransactionAbortedException;

  void postWrite(long tid, K key, boolean isDelete) throws TransactionAbortedException; 

  long getFirstActiveTID();

}