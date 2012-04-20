package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.XID;

public interface XAStateTransitionLogRecord extends StateTransitionLogRecord {

  XID getTID();

}