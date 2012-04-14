package edu.illinois.htx.test;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.htx.tm.AbstractLog;
import edu.illinois.htx.tm.LogRecord.Type;

public class StringKeyLog extends
    AbstractLog<StringKey, StringKeyLogRecord> {

  private NavigableMap<Long, StringKeyLogRecord> log;
  private long seqid;

  public StringKeyLog() {
    this.log = new TreeMap<Long, StringKeyLogRecord>();
    this.seqid = 0;
  }

  @Override
  public StringKeyLogRecord newRecord(Type type, long tid, StringKey key,
      long version) {
    return new StringKeyLogRecord(seqid++, tid, type, key, version);
  }

  @Override
  public void append(StringKeyLogRecord record) {
    log.put(record.getSID(), record);
  }

  @Override
  public void savepoint(long sid) {
    log = new TreeMap<Long, StringKeyLogRecord>(log.tailMap(sid, true));
  }

  @Override
  public Iterable<StringKeyLogRecord> start() throws IOException {
    return log.values();
  }

  @Override
  public void stop() throws IOException {
  }

}
