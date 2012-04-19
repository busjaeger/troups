package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.Key;

public interface GetLogRecord<K extends Key> extends OperationLogRecord<K> {

  long getVersion();

}
