package edu.illinois.troups.tmg;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;

public interface GroupTransactionManager<K extends Key> extends CoprocessorProtocol {

  public static final long VERSION = 1L;

  TID begin(K groupKey) throws IOException;

  void commit(K groupKey, TID tid) throws TransactionAbortedException, IOException;

  void abort(K groupKey, TID tid) throws IOException;

}