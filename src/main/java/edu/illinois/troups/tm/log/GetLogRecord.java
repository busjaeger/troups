package edu.illinois.troups.tm.log;

import edu.illinois.troups.tm.Key;

public interface GetLogRecord<K extends Key> extends OperationLogRecord<K> {

  long getVersion();

}
