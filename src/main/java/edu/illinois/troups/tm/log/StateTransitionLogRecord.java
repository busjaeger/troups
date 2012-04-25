package edu.illinois.troups.tm.log;

import edu.illinois.troups.tm.Key;

public interface StateTransitionLogRecord<K extends Key> extends LogRecord<K> {

  int getTransactionState();

}