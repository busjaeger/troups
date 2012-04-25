package edu.illinois.troup.tm;

import java.io.IOException;

public interface CrossGroupTransactionManager<K extends Key> {

  public static final long VERSION = 1L;

  XID join(TID tid, K groupKey) throws IOException;

  void prepare(XID xid) throws TransactionAbortedException, IOException;

  void commit(XID xid, boolean onePhase) throws IOException;

  void abort(XID xid) throws IOException;

}