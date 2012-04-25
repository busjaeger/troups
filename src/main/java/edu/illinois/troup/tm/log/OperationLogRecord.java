package edu.illinois.troup.tm.log;

import edu.illinois.troup.tm.Key;

public interface OperationLogRecord<K extends Key> extends LogRecord<K> {

  K getKey();

}