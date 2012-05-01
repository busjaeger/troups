package edu.illinois.troups.tm.region;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XID;

public interface CrossGroupTransactionManager<K extends Key> extends
    CoprocessorProtocol {

  public static final long VERSION = 1L;

  /**
   * 
   * @param groupKey
   * @param tid
   * @return
   * @throws TransactionAbortedException
   *           if the transaction is a straggler and was denied entry
   * @throws IOException
   */
  XID join(K groupKey, TID tid) throws TransactionAbortedException, IOException;

  void prepare(K groupKey, XID xid) throws TransactionAbortedException,
      IOException;

  void commit(K groupKey, XID xid, boolean onePhase) throws IOException;

  void abort(K groupKey, XID xid) throws IOException;

}