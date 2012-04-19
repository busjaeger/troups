package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.Key;

public interface OperationLogRecord<K extends Key> extends LogRecord {

  K getKey();
}