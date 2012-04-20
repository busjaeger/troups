package edu.illinois.htx.test;

import edu.illinois.htx.tm.log.LogRecord;

public class StringKeyLogRecord implements LogRecord {

  @Override
  public long getSID() {
    return 0;
  }

  @Override
  public long getTID() {
    return 0;
  }

  @Override
  public int getType() {
    return 0;
  }
}
