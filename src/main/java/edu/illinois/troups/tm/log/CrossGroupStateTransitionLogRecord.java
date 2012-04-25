package edu.illinois.troups.tm.log;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.XID;

public interface CrossGroupStateTransitionLogRecord<K extends Key> extends
    StateTransitionLogRecord<K> {

  XID getTID();

}