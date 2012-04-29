package edu.illinois.troups.tm.log;

import java.io.IOException;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.TransactionLog.Record;

public interface XATransactionLog<K extends Key, R extends Record<K>> extends
    TransactionLog<K, R> {

  public static final int RECORD_TYPE_XA_STATE_TRANSITION = 5;

  public interface XAStateTransitionRecord<K extends Key> extends
      StateTransitionRecord<K> {
    /**
     * XA ID of the log record
     */
    XID getTID();
  }

  public long appendXAStateTransition(XID xid, int state) throws IOException;

}
