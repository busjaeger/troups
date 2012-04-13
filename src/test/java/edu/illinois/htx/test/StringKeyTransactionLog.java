package edu.illinois.htx.test;

import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.htx.tm.AbstractTransactionLog;
import edu.illinois.htx.tm.TransactionLog.Record.Type;

public class StringKeyTransactionLog extends AbstractTransactionLog<StringKey> {

  private NavigableMap<Long, Record<StringKey>> log;
  private long seqid;

  public StringKeyTransactionLog() {
    this.log = new TreeMap<Long, Record<StringKey>>();
    this.seqid = 0;
  }

  @Override
  public Record<StringKey> newRecord(Type type, long tid, StringKey key,
      long version) {
    return new StringKeyRecord(seqid++, tid, type, key, version);
  }

  @Override
  public long append(Record<StringKey> record) {
    log.put(record.getSID(), record);
    return record.getSID();
  }

  @Override
  public Iterable<Record<StringKey>> read() {
    return log.values();
  }

  @Override
  public void savepoint(long sid) {
    log = new TreeMap<Long, Record<StringKey>>(log.tailMap(sid, true));
  }

}
