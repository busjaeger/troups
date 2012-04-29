package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.tm.XATransactionLog.RECORD_TYPE_XA_STATE_TRANSITION;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.XATransactionLog.XAStateTransitionRecord;
import edu.illinois.troups.tm.XID;

public class HXAStateTransitionRecord extends HStateTransitionRecord implements
    XAStateTransitionRecord<HKey> {

  HXAStateTransitionRecord() {
    super(RECORD_TYPE_XA_STATE_TRANSITION);
  }

  HXAStateTransitionRecord(XID xid, int state) {
    super(RECORD_TYPE_XA_STATE_TRANSITION, xid, state);
  }

  @Override
  public XID getTID() {
    return (XID) super.getTID();
  }

  protected TID createTID() {
    return new XID();
  }
}
