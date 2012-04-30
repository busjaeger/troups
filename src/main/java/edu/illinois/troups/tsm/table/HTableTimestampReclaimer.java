package edu.illinois.troups.tsm.table;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableTimestampReclaimer implements Runnable {

  private static final Log LOG = LogFactory
      .getLog(HTableTimestampReclaimer.class);

  private final HTableTimestampManager tsm;

  public HTableTimestampReclaimer(HTableTimestampManager tsm) {
    this.tsm = tsm;
  }

  @Override
  public void run() {
    long before = System.currentTimeMillis();

    ResultScanner timestamps;
    try {
      timestamps = tsm.getTimestamps();
    } catch (IOException e) {
      LOG.error("failed to get timestamps", e);
      e.printStackTrace(System.out);
      return;
    }

    ArrayList<Result> reclaimables = new ArrayList<Result>();
    for (Result timestamp : timestamps) {
      if (tsm.isReclaimable(timestamp))
        reclaimables.add(timestamp);
      else
        break;
    }

    if (!reclaimables.isEmpty()) {
      Result last = reclaimables.get(reclaimables.size() - 1);
      long reclaimed = Bytes.toLong(last.getRow());
      try {
        tsm.setLastReclaimedTimestamp(reclaimed);
      } catch (IOException e) {
        LOG.error("Failed to set last reclaimed", e);
        e.printStackTrace(System.out);
      }
      for (Result timestamp : reclaimables) {
        try {
          tsm.deleteTimestamp(timestamp);
        } catch (IOException e) {
          LOG.error("Failed to delete reclaimed timestamps", e);
          e.printStackTrace(System.out);
        }
      }
    }

    LOG.info("Timestamp collection took: "
        + (System.currentTimeMillis() - before));
  }

}
