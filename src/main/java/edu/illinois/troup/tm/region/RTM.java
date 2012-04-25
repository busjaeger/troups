package edu.illinois.troup.tm.region;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.troup.tm.GroupTransactionManager;
import edu.illinois.troup.tm.CrossGroupTransactionManager;

public interface RTM extends CoprocessorProtocol,
    GroupTransactionManager<HKey>, CrossGroupTransactionManager<HKey> {
  public static final long VERSION = 1L;

}
