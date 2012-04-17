package edu.illinois.htx.test;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import edu.illinois.htx.tsm.TimestampListener;
import edu.illinois.htx.tsm.TimestampManager;

public class InMemoryTimestampManager implements TimestampManager {

  private final List<CompletionListener> listeners = new CopyOnWriteArrayList<CompletionListener>();

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
  public void done(long ts) {
    Boolean isDone = timestamps.get(ts);
    if (isDone == null || isDone == true)
      return;
    timestamps.put(ts, true);
    if (ts > lastDeleted)
      for (long i = ts - 1; i > lastDeleted; i--)
        if (!timestamps.get(i))
          return;
    lastDeleted = ts;
    timestamps = new TreeMap<Long, Boolean>(timestamps.tailMap(ts, false));
    for (CompletionListener listener : listeners)
      listener.lastDeletedTimestampChanged(ts);
  }

  @Override
  public long getLastDeletedTimestamp() {
    return lastDeleted;
  }

  @Override
  public void addDeleteListener(CompletionListener listener) {
    listeners.add(listener);
  }

}