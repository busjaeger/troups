package edu.illinois.htx.tm.log;

public interface StateTransitionLogRecord extends LogRecord {

  int getTransactionState();

}