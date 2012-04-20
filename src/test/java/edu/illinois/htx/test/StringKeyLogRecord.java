package edu.illinois.htx.test;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.log.LogRecord;

public class StringKeyLogRecord implements LogRecord {

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
}
