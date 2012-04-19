package edu.illinois.htx.tm;

public interface ObservingXATransactionManager<K extends Key> extends
    TransactionOperationObserver<K>, XATransactionManager {

}