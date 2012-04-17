package edu.illinois.htx.tsm;

import java.io.IOException;

/**
 * TODO consider changing to more generic interface:
 * <p>
 * void setState(long ts, TimestampState state) throws IOException;
 * <p>
 * TimestampState getState(long ts) throws IOException;
 * <p>
 * void disconnect(long ts) throws IOException;
 * <p>
 * public interface ConnectionListener { void disconnected(long ts); }
 * 
 */
public interface TimestampManager {

  public interface TimestampListener {
    void deleted(long ts);
  }

  long next() throws IOException;

  void done(long ts) throws IOException;

  public boolean isDone(long ts) throws NoSuchTimestampException, IOException;

  void delete(long ts) throws NoSuchTimestampException, IOException;

  // returns all existing time-stamps in sorted order
  Iterable<Long> getTimestamps() throws IOException;

  long getLastDeletedTimestamp() throws IOException;

  void setLastDeletedTimestamp(long ts) throws IOException;

  void addLastDeletedTimestampListener(TimestampListener listener);

}