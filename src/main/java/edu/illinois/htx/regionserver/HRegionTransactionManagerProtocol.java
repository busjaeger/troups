package edu.illinois.htx.regionserver;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.htx.tm.TransactionManager;

public interface HRegionTransactionManagerProtocol extends CoprocessorProtocol,
    TransactionManager {
  public static final long VERSION = 1L;
}
