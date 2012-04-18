package edu.illinois.htx.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.illinois.htx.tsm.TimestampManager;

public class InMemoryTimestampManager implements TimestampManager {

  private final SortedMap<Long, List<TimestampListener>> timestamps = new TreeMap<Long, List<TimestampListener>>();
  private final List<TimestampReclamationListener> reclamationListeners = new ArrayList<TimestampReclamationListener>();
  private long lastActive;

  @Override
  public long create() {
    lastActive = timestamps.size();
    timestamps.put(lastActive, new ArrayList<TimestampListener>());
    return lastActive;
  }

  @Override
  public boolean delete(long ts) throws IOException {
    List<TimestampListener> listeners = timestamps.remove(ts);
    if (listeners != null) {
      for (TimestampListener listener : listeners)
        listener.deleted(ts);
      if (timestamps.isEmpty() || ts < timestamps.firstKey())
        for (TimestampReclamationListener listener : reclamationListeners)
          listener.reclaimed(getLastReclaimedTimestamp());
      return true;
    }
    return false;
  }

  @Override
  public boolean addTimestampListener(long ts, TimestampListener listener)
      throws IOException {
    List<TimestampListener> tsListeners = timestamps.get(ts);
    if (tsListeners == null)
      return false;
    tsListeners.add(listener);
    return true;
  }

  @Override
  public long getLastReclaimedTimestamp() throws IOException {
    return timestamps.isEmpty() ? lastActive : timestamps.firstKey() - 1;
  }

  @Override
  public void addTimestampReclamationListener(
      TimestampReclamationListener listener) {
    reclamationListeners.add(listener);
  }

  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

}