package edu.illinois.htx.tm;


public interface MultiversionTransactionManager<K extends Key> extends
    TransactionManager {

  void filterReads(long tid, Iterable<? extends KeyVersion<K>> versions) throws TransactionAbortedException;

  void preWrite(long tid, boolean isDelete, Iterable<? extends K> key) throws TransactionAbortedException;

  void postWrite(long tid, boolean isDelete, Iterable<? extends K> key) throws TransactionAbortedException; 

  long getFirstActiveTID();

}