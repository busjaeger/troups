package edu.illinois.troups.tsm.server;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troups.tsm.TimestampManager;

public class TimestampManagerImpl implements TimestampManager, Runnable {

  protected final ConcurrentNavigableMap<Long, Timestamp> timestamps = new ConcurrentSkipListMap<Long, Timestamp>();

  protected static final byte[] counterRow = new byte[] { 0, 0, 0, 0, 0, 0, 0,
      0, 0 };
  protected static final byte[] counterColumn = Bytes.toBytes("ctr");

  protected final HTablePool tablePool;
  protected final byte[] tableName;
  protected final byte[] familyName;
  protected final AtomicLong counter;
  protected final AtomicLong lastReclaimed;
  protected long persistedCounter;
  protected final long batchSize;
  protected final ReadWriteLock persistLock = new ReentrantReadWriteLock();
  protected final long tsTimeout;

  public TimestampManagerImpl(HTablePool tablePool, byte[] tableName,
      byte[] familyName, long batchSize, long tsTimeout) throws IOException {
    this.tablePool = tablePool;
    this.tableName = tableName;
    this.familyName = familyName;
    this.persistedCounter = readCounter();
    this.counter = new AtomicLong(persistedCounter);
    this.lastReclaimed = new AtomicLong(persistedCounter);
    this.batchSize = batchSize;
    this.tsTimeout = tsTimeout;
  }

  @Override
  public long acquire() throws IOException {
    long timestamp = counter.getAndIncrement();
    timestamps.put(timestamp, newTimestamp());
    boolean needWrite = false;
    persistLock.readLock().lock();
    try {
      if (timestamp >= persistedCounter)
        needWrite = true;
    } finally {
      persistLock.readLock().unlock();
    }
    if (needWrite) {
      persistLock.writeLock().lock();
      try {
        if (timestamp >= persistedCounter) {
          long newValue = persistedCounter + batchSize;
          writeTimestamp(newValue);
          persistedCounter = newValue;
        }
      } finally {
        persistLock.writeLock().unlock();
      }
    }
    return timestamp;
  }

  protected Timestamp newTimestamp() {
    return new Timestamp();
  }

  @Override
  public boolean release(long ts) throws IOException {
    Timestamp timestamp = timestamps.get(ts);
    if (timestamp == null)
      return false;
    return timestamp.release();
  }

  @Override
  public long getLastReclaimedTimestamp() throws IOException {
    return lastReclaimed.get();
  }

  @Override
  public void addTimestampReclamationListener(
      TimestampReclamationListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

  void writeTimestamp(long ts) throws IOException {
    HTableInterface table = tablePool.getTable(tableName);
    try {
      Put put = new Put(counterRow);
      put.add(familyName, counterColumn, Bytes.toBytes(ts));
      table.put(put);
    } finally {
      table.close();
    }
  }

  long readCounter() throws IOException {
    HTableInterface table = tablePool.getTable(tableName);
    try {
      Get get = new Get(counterRow);
      get.addColumn(familyName, counterColumn);
      Result result = table.get(get);
      byte[] value = result.getValue(familyName, counterColumn);
      return value == null ? 0 : Bytes.toLong(value);
    } finally {
      table.close();
    }
  }

  // timestamp reclamation
  @Override
  public void run() {
    Long reclaimed = null;
    Iterator<Entry<Long, Timestamp>> it = timestamps.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Long, Timestamp> entry = it.next();
      Timestamp ts = entry.getValue();
      if (ts.isReleased()) {
        it.remove();
        reclaimed = entry.getKey();
        continue;
      }
      if (ts.isPersisted())
        continue;
      if (System.currentTimeMillis() - ts.getCreationTime() > tsTimeout) {
        it.remove();
        reclaimed = entry.getKey();
        continue;
      }
    }
    if (reclaimed != null)
      lastReclaimed.set(reclaimed);
  }
}
