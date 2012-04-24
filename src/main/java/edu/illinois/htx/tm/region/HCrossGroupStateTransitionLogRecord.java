package edu.illinois.htx.tm.region;

import static edu.illinois.htx.tm.log.CrossGroupLog.RECORD_TYPE_XG_STATE_TRANSITION;
import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.XID;
import edu.illinois.htx.tm.log.CrossGroupStateTransitionLogRecord;

public class HCrossGroupStateTransitionLogRecord extends
    HStateTransitionLogRecord implements
    CrossGroupStateTransitionLogRecord<HKey> {

  HCrossGroupStateTransitionLogRecord() {
    super(RECORD_TYPE_XG_STATE_TRANSITION);
  }

  HCrossGroupStateTransitionLogRecord(long sid, XID xid, HKey groupKey,
      int state) {
    super(RECORD_TYPE_XG_STATE_TRANSITION, sid, xid, groupKey, state);
  }

  @Override
  public XID getTID() {
    return (XID) super.getTID();
  }

  protected TID createTID() {
    return new XID();
  }
}
