package edu.illinois.htx.test;

import edu.illinois.htx.tm.LogRecord;

public class StringKeyLogRecord implements LogRecord<StringKey> {

  private final long sid;
  private final long tid;
  private final Type type;
  private final StringKey key;
  private final Long version;

  public StringKeyLogRecord(long sid, long tid, Type type, StringKey key,
      Long version) {
    this.sid = sid;
    this.tid = tid;
    this.type = type;
    this.key = key;
    this.version = version;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public long getSID() {
    return sid;
  }

  @Override
  public long getTID() {
    return tid;
  }

  @Override
  public StringKey getKey() {
    return key;
  }

  @Override
  public long getVersion() {
    return version;
  }

}
