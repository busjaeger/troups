package edu.illinois.troups.tmg;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XID;

public interface CrossGroupTransactionManager<K extends Key> extends
    CoprocessorProtocol {

  public static final long VERSION = 1L;

  XID join(K groupKey, TID tid) throws IOException;

  void prepare(K groupKey, XID xid) throws TransactionAbortedException,
      IOException;

  void commit(K groupKey, XID xid, boolean onePhase) throws IOException;

  void abort(K groupKey, XID xid) throws IOException;

}