package edu.illinois.troup.tm.log;

import edu.illinois.troup.tm.Key;

public interface GetLogRecord<K extends Key> extends OperationLogRecord<K> {

  long getVersion();

}
