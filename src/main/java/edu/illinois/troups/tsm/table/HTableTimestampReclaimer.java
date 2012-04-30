package edu.illinois.troups.tsm.table;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HTableTimestampReclaimer implements Runnable {

  private static final Log LOG = LogFactory
      .getLog(HTableTimestampReclaimer.class);

  private final HTableTimestampManager tsm;

  public HTableTimestampReclaimer(HTableTimestampManager tsm) {
    this.tsm = tsm;
  }

  @Override
  public void run() {
    NavigableMap<Long, NavigableMap<byte[], byte[]>> timestamps;
    try {
      timestamps = tsm.getTimestamps();
    } catch (IOException e) {
      LOG.error("failed to get timestamps", e);
      e.printStackTrace(System.out);
      return;
    }
    if (timestamps.isEmpty())
      return;
    Long reclaimed = null;
    for (Entry<Long, NavigableMap<byte[], byte[]>> ts : timestamps.entrySet()) {
      NavigableMap<byte[], byte[]> columns = ts.getValue();
      if (tsm.isReclaimable(columns))
        reclaimed = ts.getKey();
      else
        break;
    }
    if (reclaimed != null) {
      try {
        tsm.setLastReclaimedTimestamp(reclaimed);
      } catch (IOException e) {
        LOG.error("Failed to set last reclaimed", e);
        e.printStackTrace(System.out);
      }
      try {
        tsm.deleteTimestamps(reclaimed);
      } catch (IOException e) {
        LOG.error("Failed to delete reclaimed timestamps", e);
        e.printStackTrace(System.out);
      }
    }
  }

}
