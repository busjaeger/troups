package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.XID;

public interface CrossGroupStateTransitionLogRecord<K extends Key> extends
    StateTransitionLogRecord<K> {

  XID getTID();

}