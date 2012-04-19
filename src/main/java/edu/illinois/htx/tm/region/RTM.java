package edu.illinois.htx.tm.region;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.htx.tm.TransactionManager;
import edu.illinois.htx.tm.XATransactionManager;

public interface RTM extends CoprocessorProtocol,
    TransactionManager, XATransactionManager {
  public static final long VERSION = 1L;

}
