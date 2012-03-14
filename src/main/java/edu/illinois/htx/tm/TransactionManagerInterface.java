package edu.illinois.htx.tm;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;

public interface TransactionManagerInterface extends VersionedProtocol, Stoppable {

  public static final long VERSION = 1L;

  long begin();

  void commit(long tid) throws TransactionAbortedException;

  void abort(long tid);

}