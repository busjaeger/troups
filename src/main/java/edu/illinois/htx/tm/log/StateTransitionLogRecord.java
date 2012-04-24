package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.Key;

public interface StateTransitionLogRecord<K extends Key> extends LogRecord<K> {

  int getTransactionState();

}