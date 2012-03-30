package edu.illinois.htx.regionserver;

import java.util.Collections;

import edu.illinois.htx.tm.AbstractTransactionLog;
import edu.illinois.htx.tm.TransactionLog.Entry.Type;

// TODO implement
public class HRegionTransactionLog extends AbstractTransactionLog<HKey> {

  @Override
  public Entry<HKey> newEntry(Type type, long tid, HKey key, long version) {
    return null;
  }

  @Override
  public void append(Entry<HKey> entry) {
  }

  @Override
  public Iterable<Entry<HKey>> read() {
    return Collections.emptyList();
  }

}
