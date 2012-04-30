package edu.illinois.troups.tsm.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.illinois.troups.tsm.NoSuchTimestampException;

public class ZKTimestampReclaimer implements Runnable {

  private final ZKTimestampManager tsm;
  private Long lastReclaimed;
  private long lastSeen;

  public ZKTimestampReclaimer(ZKTimestampManager tsm) {
    this.tsm = tsm;
  }

  @Override
  public void run() {
    try {
      if (lastReclaimed == null) {
        lastReclaimed = tsm.getLastReclaimedTimestamp();
        lastSeen = lastReclaimed;
      }

      long newLastReclaimed = lastReclaimed;
      List<Long> deletes = new ArrayList<Long>();
      List<Long> timestamps = tsm.getTimestamps();

      if (timestamps.isEmpty()) {
        // with 'lastSeen' we are not guaranteed to always clean up transactions
        // right away, but we will eventually and it performs well
        newLastReclaimed = lastSeen;
      } else {
        long last = timestamps.get(timestamps.size() - 1);
        if (last > lastSeen)
          lastSeen = last;
        for (long ts : timestamps) {
          // time-stamps that we failed to delete before
          if (ts < lastReclaimed) {
            deletes.add(ts);
          }
          // time-stamps after current lrt: check if still needed
          else if (ts >= lastReclaimed) {
            if (!tsm.isReleased(ts)) {
              lastReclaimed = ts - 1;
              break;
            }
            deletes.add(ts);
          }
        }
      }
      if (newLastReclaimed != lastReclaimed) {
        tsm.setLastReclaimedTimestamp(newLastReclaimed);
        lastReclaimed = newLastReclaimed;
      }

      for (Long delete : deletes)
        try {
          tsm.release(delete);
        } catch (NoSuchTimestampException e) {
          // ignore
        }
    } catch (IOException e) {
      e.printStackTrace(System.out);
    }
  }

}
