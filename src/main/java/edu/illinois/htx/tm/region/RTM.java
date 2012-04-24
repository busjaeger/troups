package edu.illinois.htx.tm.region;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.htx.tm.GroupTransactionManager;
import edu.illinois.htx.tm.CrossGroupTransactionManager;

public interface RTM extends CoprocessorProtocol,
    GroupTransactionManager<HKey>, CrossGroupTransactionManager<HKey> {
  public static final long VERSION = 1L;

}
