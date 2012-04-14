package edu.illinois.htx.tm;

import java.io.IOException;

import edu.illinois.htx.tm.LogRecord.Type;

public interface Log<K extends Key, R extends LogRecord<K>> {

  R newRecord(Type type, long tid, K key, long version);

  void append(R record) throws IOException;

  long appendBegin(long tid) throws IOException;

  long appendCommit(long tid) throws IOException;

  long appendAbort(long tid) throws IOException;

  long appendFinalize(long tid) throws IOException;

  long appendRead(long tid, K key, long version) throws IOException;

  long appendWrite(long tid, K key, boolean isDelete) throws IOException;

  void savepoint(long sid) throws IOException;

  Iterable<R> start() throws IOException;

  void stop() throws IOException;

}