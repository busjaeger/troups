package edu.illinois.htx.tm.region;

import static edu.illinois.htx.tm.log.XALog.RECORD_TYPE_XA_STATE_TRANSITION;
import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.XID;
import edu.illinois.htx.tm.log.XAStateTransitionLogRecord;

public class HXAStateTransitionLogRecord extends HStateTransitionLogRecord
    implements XAStateTransitionLogRecord {

  HXAStateTransitionLogRecord() {
    super(RECORD_TYPE_XA_STATE_TRANSITION);
  }

  HXAStateTransitionLogRecord(long sid, XID xid, int state) {
    super(RECORD_TYPE_XA_STATE_TRANSITION, sid, xid, state);
  }

  @Override
  public XID getTID() {
    return (XID) super.getTID();
  }

  protected TID createTID() {
    return new XID();
  }
}
