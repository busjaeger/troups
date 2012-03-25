package edu.illinois.htx.tm;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface TransactionManagerProtocol extends CoprocessorProtocol {

  void begin(long tid);

  void commit(long tid) throws TransactionAbortedException;

  void abort(long tid);

}
