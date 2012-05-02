package edu.illinois.troups.tm.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import edu.illinois.troups.tsm.TimestampManager;

// this idea and the majority of the code is taken from omid
public class VersionCollector implements InternalScanner {

  public static final Log LOG = LogFactory.getLog(VersionCollector.class);

  private final TimestampManager tsm;
  private final InternalScanner internalScanner;
  private final long lastReclaimedTimestamp;
  private Set<HKey> columnsSeen = new HashSet<HKey>();

  public VersionCollector(TimestampManager tsm,
      InternalScanner internalScanner, long lastReclaimedTimestamp) {
    this.tsm = tsm;
    this.internalScanner = internalScanner;
    this.lastReclaimedTimestamp = lastReclaimedTimestamp;
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    return next(results, -1);
  }

  @Override
  public boolean next(List<KeyValue> result, int limit) throws IOException {
    boolean moreRows = false;
    List<KeyValue> raw = new ArrayList<KeyValue>(limit);
    while (limit == -1 || result.size() < limit) {
      int toReceive = limit == -1 ? -1 : limit - result.size();
      moreRows = internalScanner.next(raw, toReceive);
      for (KeyValue kv : raw) {
        /*
         * If the version is higher than the last reclaimed timestamp, keep it
         * around, since the transaction may still be active
         */
        if (tsm.compare(lastReclaimedTimestamp, kv.getTimestamp()) < 0) {
          result.add(kv);
        } else {
          /*
           * From the remaining versions, we only need to keep the latest. Note
           * that this assumes the internal scanner returns later rows before
           * earlier ones (which is typically the case in HBase).
           */
          HKey key = new HKey(kv);
          if (columnsSeen.add(key))
            result.add(kv);
          else
            LOG.info("Discarded " + kv);
        }
      }
      if (raw.size() < toReceive || toReceive == -1) {
        columnsSeen.clear();
        break;
      }
      raw.clear();
    }
    if (!moreRows) {
      columnsSeen.clear();
    }
    return moreRows;
  }

  @Override
  public void close() throws IOException {
    internalScanner.close();
  }
}
