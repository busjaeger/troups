package edu.illinois.troups.tsm.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troups.tsm.TimestampManager;

public class HTableTimestampManager implements TimestampManager, Runnable {

  private static final Log LOG = LogFactory
      .getLog(HTableTimestampManager.class);

  // counter rows between zero and 1
  protected static final byte[] counterRow = new byte[] { 0, 0, 0, 0, 0, 0, 0,
      0, 0 };
  protected static final byte[] lastReclaimedRow = new byte[] { 0, 0, 0, 0, 0,
      0, 0, 0, 1 };

  protected static final byte[] releasedColumn = Bytes.toBytes("released");
  protected static final byte[] timeColumn = Bytes.toBytes("time");

  protected static final byte[] notReleased = Bytes.toBytes(false);
  protected static final byte[] released = Bytes.toBytes(true);

  protected final HTablePool tablePool;
  protected final byte[] tableName;
  protected final byte[] tsFamily;
  protected final ScheduledExecutorService pool;
  protected final long timestampTimeout;
  protected final List<TimestampReclamationListener> listeners = new CopyOnWriteArrayList<TimestampReclamationListener>();

  private long reclaimed = 0;

  public HTableTimestampManager(HTablePool tablePool, byte[] tableName,
      byte[] tsFamily, ScheduledExecutorService pool, long timestampTimeout) {
    this.tablePool = tablePool;
    this.tableName = tableName;
    this.tsFamily = tsFamily;
    this.pool = pool;
    this.timestampTimeout = timestampTimeout;
  }

  @Override
  public long acquire() throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      long ts = tsTable.incrementColumnValue(counterRow, tsFamily, timeColumn,
          1L);
      byte[] row = Bytes.toBytes(ts);
      Put put = new Put(row, ts);
      put.add(tsFamily, releasedColumn, notReleased);
      put.add(tsFamily, timeColumn, Bytes.toBytes(System.currentTimeMillis()));
      tsTable.put(put);
      return ts;
    } finally {
      tsTable.close();
    }
  }

  @Override
  public boolean release(long ts) throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      byte[] row = Bytes.toBytes(ts);
      Put put = new Put(row, ts);
      put.add(tsFamily, releasedColumn, released);
      boolean released = tsTable.checkAndPut(row, tsFamily, releasedColumn,
          notReleased, put);
      if (!released)
        LOG.warn("Failed to release timestamp " + ts);
      return released;
    } finally {
      tsTable.close();
    }
  }

  @Override
  public long getLastReclaimedTimestamp() throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      Get get = new Get(lastReclaimedRow);
      get.addColumn(tsFamily, null);
      Result result = tsTable.get(get);
      byte[] value = result.getValue(tsFamily, null);
      return value == null ? 0L : Bytes.toLong(value);
    } finally {
      tsTable.close();
    }
  }

  // internal
  void setLastReclaimedTimestamp(long ts) throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      Put put = new Put(lastReclaimedRow, 1l);
      put.add(tsFamily, null, Bytes.toBytes(ts));
      tsTable.put(put);
    } finally {
      tsTable.close();
    }
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
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      Get get = new Get(counterRow);
      get.setTimeStamp(1l);
      get.addColumn(tsFamily, timeColumn);
      Result result = tsTable.get(get);
      byte[] value = result.getValue(tsFamily, null);
      return value == null ? 0L : Bytes.toLong(value);
    } finally {
      tsTable.close();
    }
  }

  // reclamation
  @Override
  public void run() {
    long before = System.currentTimeMillis();
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      ArrayList<byte[]> reclaimables = new ArrayList<byte[]>();
      Scan scan = new Scan();
      ResultScanner scanner;
      try {
        scanner = tsTable.getScanner(scan);
      } catch (IOException e) {
        LOG.error("Failed to create Scanner", e);
        e.printStackTrace(System.out);
        return;
      }
      try {
        for (Result result : scanner) {
          // filter counter rows
          if (result.getValue(tsFamily, releasedColumn) == null) {
            byte[] row = result.getRow();
            if (Bytes.equals(row, counterRow)
                || Bytes.equals(row, lastReclaimedRow))
              continue;
            LOG.error("row without released column " + result.getRow());
            return;
          }
          if (isReclaimable(result))
            reclaimables.add(result.getRow());
          else
            break;
        }
      } finally {
        scanner.close();
      }

      if (!reclaimables.isEmpty()) {
        byte[] last = reclaimables.get(reclaimables.size() - 1);
        long reclaimed = Bytes.toLong(last);
        try {
          setLastReclaimedTimestamp(reclaimed);
        } catch (IOException e) {
          LOG.error("Failed to set last reclaimed", e);
          e.printStackTrace(System.out);
        }
        for (byte[] timestamp : reclaimables) {
          try {
            deleteTimestamp(timestamp);
          } catch (IOException e) {
            LOG.error("Failed to delete reclaimed timestamps", e);
            e.printStackTrace(System.out);
          }
        }
      }

    } finally {
      try {
        tsTable.close();
      } catch (IOException e) {
        LOG.error("failed to close table", e);
      }
    }
    LOG.info("Timestamp collection took: "
        + (System.currentTimeMillis() - before));
  }

  boolean isReclaimable(Result timestamp) {
    byte[] bytes = timestamp.getValue(tsFamily, releasedColumn);
    // if timestamp has been released, it can be reclaimed
    if (Bytes.toBoolean(bytes))
      return true;
    // if tiemstamp has timed out, it can be reclaimed
    // note: clock skew can cause problems here, could use ticks instead
    long time = Bytes.toLong(timestamp.getValue(tsFamily, timeColumn));
    long current = System.currentTimeMillis();
    if (current > time && (time + timestampTimeout) < current)
      return true;
    // otherwise it's still in use
    return false;
  }

  void deleteTimestamp(byte[] row) throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      Delete delete = new Delete(row);
      tsTable.delete(delete);
    } finally {
      tsTable.close();
    }
  }

}
