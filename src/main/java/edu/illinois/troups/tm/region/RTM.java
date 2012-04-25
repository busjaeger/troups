package edu.illinois.troups.tm.region;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.troups.tm.GroupTransactionManager;
import edu.illinois.troups.tm.CrossGroupTransactionManager;

public interface RTM extends CoprocessorProtocol,
    GroupTransactionManager<HKey>, CrossGroupTransactionManager<HKey> {
  public static final long VERSION = 1L;

}
