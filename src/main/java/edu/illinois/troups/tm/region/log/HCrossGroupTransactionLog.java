package edu.illinois.troups.tm.region.log;

import java.io.IOException;

import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.XATransactionLog;
import edu.illinois.troups.tm.region.HKey;

public class HCrossGroupTransactionLog extends HGroupTransactionLog implements
    XATransactionLog<HKey, HRecord> {

  public HCrossGroupTransactionLog(HKey groupKey, GroupLogStore logStore) {
    super(groupKey, logStore);
  }

  @Override
  protected HRecord create(int type) {
    switch (type) {
    case XATransactionLog.RECORD_TYPE_XA_STATE_TRANSITION:
      return new HXAStateTransitionRecord();
    default:
      return super.create(type);
    }
  }

  @Override
  public long appendXAStateTransition(XID xid, int state) throws IOException {
    HRecord record = new HXAStateTransitionRecord(xid, state);
    return append(groupKey, record);
  }
}
