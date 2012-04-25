package edu.illinois.troups.tm.log;

import edu.illinois.troups.tm.Key;

public interface OperationLogRecord<K extends Key> extends LogRecord<K> {

  K getKey();

}