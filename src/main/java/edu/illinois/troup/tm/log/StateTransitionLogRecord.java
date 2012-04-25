package edu.illinois.troup.tm.log;

import edu.illinois.troup.tm.Key;

public interface StateTransitionLogRecord<K extends Key> extends LogRecord<K> {

  int getTransactionState();

}