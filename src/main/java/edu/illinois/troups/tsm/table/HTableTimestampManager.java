package edu.illinois.troups.tsm.table;

import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troups.tsm.TimestampManager;

public class HTableTimestampManager implements TimestampManager {

  private static final Log LOG = LogFactory
      .getLog(HTableTimestampManager.class);

  protected static final byte[] counterRow = Bytes.toBytes(0);
  protected static final byte[] lastReclaimedRow = Bytes.toBytes(1);
  protected static final byte[] timestampRow = Bytes.toBytes(2);

  protected static final byte[] releasedColumn = Bytes.toBytes("released");
  protected static final byte[] timeColumn = Bytes.toBytes("time");

  protected static final byte[] notReleased = Bytes.toBytes(false);
  protected static final byte[] released = Bytes.toBytes(true);

  protected final HTable tsTable;
  protected final byte[] tsFamily;
  protected final ScheduledExecutorService pool;
  protected final long timestampTimeout;
  protected final List<TimestampReclamationListener> listeners = new CopyOnWriteArrayList<TimestampReclamationListener>();

  private long reclaimed = 0;

  public HTableTimestampManager(HTable tsTable, byte[] tsFamily,
      ScheduledExecutorService pool, long timestampTimeout) {
    this.tsTable = tsTable;
    this.tsFamily = tsFamily;
    this.pool = pool;
    this.timestampTimeout = timestampTimeout;
    // pre-create counter row
    Put put = new Put(Bytes.toBytes(3));
    put.add(tsFamily, releasedColumn, released);
    try {
      tsTable.put(put);
    } catch (IOException e) {
      LOG.error("error on initial put", e);
      e.printStackTrace(System.out);
    }
  }

  @Override
  public long acquire() throws IOException {
    long ts = tsTable.incrementColumnValue(counterRow, tsFamily, timeColumn, 1);
    Put put = new Put(timestampRow, ts);
    put.add(tsFamily, releasedColumn, notReleased);
    put.add(tsFamily, timeColumn, Bytes.toBytes(System.currentTimeMillis()));
    tsTable.put(put);
    return ts;
  }

  @Override
  public boolean release(long ts) throws IOException {
    Put put = new Put(timestampRow, ts);
    put.add(tsFamily, releasedColumn, released);
    boolean released =tsTable.checkAndPut(timestampRow, tsFamily, releasedColumn,
        notReleased, put);
    if (!released)
      LOG.warn("Failed to release timestamp "+ts);
    return released;
  }

  @Override
  public long getLastReclaimedTimestamp() throws IOException {
    Get get = new Get(lastReclaimedRow);
    get.addColumn(tsFamily, null);
    Result result = tsTable.get(get);
    byte[] value = result.getValue(tsFamily, null);
    return value == null ? 0L : Bytes.toLong(value);
  }

  // internal
  void setLastReclaimedTimestamp(long ts) throws IOException {
    Put put = new Put(lastReclaimedRow, 1l);
    put.add(tsFamily, null, Bytes.toBytes(ts));
    tsTable.put(put);
  }

  @Override
  public void addTimestampReclamationListener(
      TimestampReclamationListener listener) {
    if (pool == null)
      throw new IllegalStateException("No pool configured");
    synchronized (listeners) {
      if (listeners.isEmpty()) {
        try {
          reclaimed = getLastReclaimedTimestamp();
        } catch (IOException e1) {
          throw new RuntimeException(e1);
        }
        pool.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            long newReclaimed;
            try {
              newReclaimed = getLastReclaimedTimestamp();
            } catch (IOException e) {
              LOG.error("failed to get reclaimed timestamp", e);
              e.printStackTrace(System.out);
              return;
            }
            if (compare(reclaimed, newReclaimed) < 0)
              for (TimestampReclamationListener listener : listeners)
                listener.reclaimed(newReclaimed);
            reclaimed = newReclaimed;
          }
        }, 0, 5, TimeUnit.SECONDS);// TODO make configurable
      }
    }
    listeners.add(listener);
  }

  // TODO handle overflow
  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

  // internal
  long getLastCreatedTimestamp() throws IOException {
    Get get = new Get(counterRow);
    get.setTimeStamp(1l);
    get.addColumn(tsFamily, timeColumn);
    Result result = tsTable.get(get);
    byte[] value = result.getValue(tsFamily, null);
    return value == null ? 0L : Bytes.toLong(value);
  }

  // internal
  NavigableMap<Long, NavigableMap<byte[], byte[]>> getTimestamps()
      throws IOException {
    Get get = new Get(timestampRow);
    get.addFamily(tsFamily);
    Result result = tsTable.get(get);
    NavigableMap<Long, NavigableMap<byte[], byte[]>> timestamps = new TreeMap<Long, NavigableMap<byte[], byte[]>>();
    if (result.getMap().isEmpty())
      return timestamps;
    NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = result.getMap()
        .get(timestampRow);
    for (Entry<byte[], NavigableMap<Long, byte[]>> column : columns.entrySet()) {
      byte[] c = column.getKey();
      for (Entry<Long, byte[]> version : column.getValue().entrySet()) {
        Long ts = version.getKey();
        NavigableMap<byte[], byte[]> tsMap = timestamps.get(ts);
        if (tsMap == null)
          timestamps.put(ts, tsMap = new TreeMap<byte[], byte[]>(
              BYTES_COMPARATOR));
        tsMap.put(c, version.getValue());
      }
    }
    return timestamps;
  }

  boolean isReclaimable(NavigableMap<byte[], byte[]> timestamp) {
    byte[] bytes = timestamp.get(releasedColumn);
    // if timestamp has been released, it can be reclaimed
    if (Bytes.toBoolean(bytes))
      return true;
    // if tiemstamp has timed out, it can be reclaimed
    // note: clock skew can cause problems here, could use ticks instead
    long time = Bytes.toLong(timestamp.get(timeColumn));
    long current = System.currentTimeMillis();
    if (current > time && (time + timestampTimeout) < current)
      return true;
    // otherwise it's still in use
    return false;
  }

  void deleteTimestamps(long ts) throws IOException {
    Delete delete = new Delete(timestampRow);
    delete.deleteFamily(tsFamily, ts);
    tsTable.delete(delete);
  }
}
