package edu.illinois.htx.regionserver;

import java.util.Collections;

import edu.illinois.htx.tm.AbstractTransactionLog;
import edu.illinois.htx.tm.TransactionLog.Record.Type;

// TODO implement
public class HRegionTransactionLog extends AbstractTransactionLog<HKey> {

  @Override
  public Record<HKey> newRecord(Type type, long tid, HKey key, long version) {
    return null;
  }

  @Override
  public long append(Record<HKey> entry) {
    return entry.getSID();
  }

  @Override
  public Iterable<Record<HKey>> read() {
    return Collections.emptyList();
  }

  @Override
  public void savepoint(long sid) {
  }

}
