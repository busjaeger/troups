package edu.illinois.htx.regionserver;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.htx.tm.TransactionManager;

public interface CoprocessorTransactionManagerProtocol extends CoprocessorProtocol,
    TransactionManager {

}
