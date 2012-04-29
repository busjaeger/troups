package edu.illinois.troups.test;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog.Record;

public class StringKeyLogRecord implements Record<StringKey> {

  @Override
  public TID getTID() {
    return null;
  }

  @Override
  public int getType() {
    return 0;
  }

}
