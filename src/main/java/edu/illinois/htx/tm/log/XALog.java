package edu.illinois.htx.tm.log;

import java.io.IOException;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.XID;

public interface XALog<K extends Key, R extends LogRecord> extends Log<K, R> {

  public static final int RECORD_TYPE_XA_STATE_TRANSITION = 5;

  public long appendXAStateTransition(XID xid, int state) throws IOException;

}
