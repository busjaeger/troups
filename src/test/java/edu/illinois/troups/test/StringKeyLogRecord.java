package edu.illinois.troups.test;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.LogRecord;

public class StringKeyLogRecord implements LogRecord<StringKey> {

  @Override
  public long getSID() {
    return 0;
  }

  @Override
  public TID getTID() {
    return null;
  }

  @Override
  public int getType() {
    return 0;
  }

  @Override
  public StringKey getGroupKey() {
    return null;
  }
}
