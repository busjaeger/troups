package edu.illinois.htx.tm;

public interface ObservingTransactionManager<K extends Key> extends
    TransactionManager, TransactionOperationObserver<K> {

}
