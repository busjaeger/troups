package edu.illinois.troups.tm.log;

import java.io.IOException;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.XID;

public interface CrossGroupLog<K extends Key, R extends LogRecord<K>> extends
    Log<K, R> {

  public static final int RECORD_TYPE_XG_STATE_TRANSITION = 5;

  public long appendCrossGroupStateTransition(XID xid, K groupKey, int state)
      throws IOException;

}
