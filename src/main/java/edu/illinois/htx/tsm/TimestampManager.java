package edu.illinois.htx.tsm;

import java.io.IOException;

public interface TimestampManager {

  long next() throws IOException;

  void done(long ts) throws IOException;

  long getLastDeletedTimestamp();

  void addTimestampListener(TimestampListener listener);

}