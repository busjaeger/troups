package edu.illinois.troup.tm.log;

import edu.illinois.troup.tm.Key;
import edu.illinois.troup.tm.XID;

public interface CrossGroupStateTransitionLogRecord<K extends Key> extends
    StateTransitionLogRecord<K> {

  XID getTID();

}