package edu.illinois.troups.tsm.mem;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.TimestampManager;

public class InMemoryTimestampManager implements TimestampManager {

  private final SortedMap<Long, List<TimestampListener>> timestamps = new ConcurrentSkipListMap<Long, List<TimestampListener>>();
  private final List<TimestampReclamationListener> reclamationListeners = new CopyOnWriteArrayList<TimestampReclamationListener>();
  private final AtomicLong lastActive = new AtomicLong();

  public InMemoryTimestampManager() {
    this(0);
  }

  public InMemoryTimestampManager(long lastActive) {
    this.lastActive.set(lastActive);
  }

  @Override
  public long acquire() {
    long ts = lastActive.getAndIncrement();
    timestamps.put(ts, new CopyOnWriteArrayList<TimestampListener>());
    return ts;
  }

  @Override
  public boolean release(long ts) throws IOException {
    List<TimestampListener> listeners = timestamps.remove(ts);
    if (listeners != null) {
      for (TimestampListener listener : listeners)
        listener.released(ts);
      boolean lastReclaimedChanged;
      try {
        lastReclaimedChanged = timestamps.isEmpty()
            || ts < timestamps.firstKey();
      } catch (NoSuchElementException e) {
        lastReclaimedChanged = true;
      }
      if (lastReclaimedChanged)
        for (TimestampReclamationListener listener : reclamationListeners)
          listener.reclaimed(getLastReclaimedTimestamp());
      return true;
    }
    return false;
  }

  // @Override
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
    long last = lastActive.get();
    if (timestamps.isEmpty())
      return last;
    try {
      return timestamps.firstKey() - 1;
    } catch (NoSuchElementException e) {
      return last;
    }
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

  // @Override
  public boolean isHeldByCaller(long ts) throws NoSuchTimestampException,
      IOException {
    // in-memory always held by caller if present
    return !isReleased(ts);
  }

  @Override
  public boolean isReleased(long ts) throws IOException {
    return !timestamps.containsKey(ts);
  }

}