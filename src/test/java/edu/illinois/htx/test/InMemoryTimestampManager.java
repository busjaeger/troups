package edu.illinois.htx.test;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.TimestampManager;

public class InMemoryTimestampManager implements TimestampManager {

  private final List<TimestampListener> listeners = new CopyOnWriteArrayList<TimestampListener>();

  private NavigableMap<Long, Boolean> timestamps = new TreeMap<Long, Boolean>();
  private long lastActive = 0;
  private long lastDeleted = -1;

  @Override
  public long next() {
    long next = lastActive++;
    timestamps.put(next, false);
    return next;
  }

  @Override
  public boolean isDone(long ts) throws IOException {
    Boolean isDone = timestamps.get(ts);
    return isDone == null || isDone == true;
  }

  @Override
  public void done(long ts) throws IOException {
    if (isDone(ts))
      return;
    timestamps.put(ts, true);
    if (ts > lastDeleted)
      for (long i = ts - 1; i > lastDeleted; i--)
        if (!timestamps.get(i))
          return;
    lastDeleted = ts;
    timestamps = new TreeMap<Long, Boolean>(timestamps.tailMap(ts, false));
    for (TimestampListener listener : listeners)
      listener.deleted(ts);
  }

  @Override
  public long getLastDeletedTimestamp() {
    return lastDeleted;
  }

  @Override
  public void delete(long ts) throws NoSuchTimestampException, IOException {
    timestamps.remove(ts);
  }

  @Override
  public Iterable<Long> getTimestamps() throws IOException {
    return timestamps.keySet();
  }

  @Override
  public void setLastDeletedTimestamp(long ts) throws IOException {
    this.lastActive = ts;
  }

  @Override
  public void addLastDeletedTimestampListener(TimestampListener listener) {
    listeners.add(listener);
  }
}