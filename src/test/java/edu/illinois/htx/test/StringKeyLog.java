package edu.illinois.htx.test;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.htx.tm.Log;
import edu.illinois.htx.tm.LogRecord.Type;

public class StringKeyLog extends Log<StringKey, StringKeyLogRecord> {

  private NavigableMap<Long, StringKeyLogRecord> log;
  private long seqid;

  public StringKeyLog() {
    this.log = new TreeMap<Long, StringKeyLogRecord>();
    this.seqid = 0;
  }

  @Override
  public StringKeyLogRecord newRecord(Type type, long tid, StringKey key,
      Long version, Long pid) {
    return new StringKeyLogRecord(seqid++, tid, type, key, version, pid);
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
  public Iterable<StringKeyLogRecord> recover() throws IOException {
    return log.values();
  }

}
