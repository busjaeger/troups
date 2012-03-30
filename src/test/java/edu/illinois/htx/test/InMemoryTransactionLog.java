package edu.illinois.htx.test;

import edu.illinois.htx.tm.AbstractTransactionLog;
import edu.illinois.htx.tm.TransactionLog.Entry.Type;

public class InMemoryTransactionLog extends AbstractTransactionLog<StringKey> {

  @Override
  public Entry<StringKey> newEntry(Type type, long tid, StringKey key,
      long version) {
    return null;
  }

  @Override
  public void append(Entry<StringKey> entry) {
  }

  @Override
  public Iterable<Entry<StringKey>> read() {
    return null;
  }

}
