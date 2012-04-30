package edu.illinois.troups.tsm.table;

import static edu.illinois.troups.Constants.DEFAULT_TSS_TABLE_FAMILY_NAME;
import static edu.illinois.troups.Constants.DEFAULT_TSS_TABLE_NAME;
import static edu.illinois.troups.Constants.DEFAULT_TSS_TIMESTAMP_TIMEOUT;
import static edu.illinois.troups.Constants.TSS_TABLE_FAMILY_NAME;
import static edu.illinois.troups.Constants.TSS_TABLE_NAME;
import static edu.illinois.troups.Constants.TSS_TIMESTAMP_TIMEOUT;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import edu.illinois.troups.tm.region.HRegionTransactionManager;
import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.NotOwnerException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class HTableSharedTimestampManager extends HTableTimestampManager
    implements SharedTimestampManager {

  private static final Log LOG = LogFactory
      .getLog(HTableSharedTimestampManager.class);

  protected static byte[] referenceCounter = Bytes.toBytes("refcnt");
  protected static byte[] references = Bytes.toBytes("refs");

  public static HTableSharedTimestampManager newInstance(
      HConnection connection, ScheduledExecutorService pool) throws IOException {
    Configuration conf = connection.getConfiguration();
    byte[] tsTableName = toBytes(conf.get(TSS_TABLE_NAME,
        DEFAULT_TSS_TABLE_NAME));
    byte[] tsFamilyName = toBytes(conf.get(TSS_TABLE_FAMILY_NAME,
        DEFAULT_TSS_TABLE_FAMILY_NAME));
    HTable tsTable = HRegionTransactionManager.demandTable(connection, pool,
        tsTableName, tsFamilyName);
    long tsTimeout = conf.getLong(TSS_TIMESTAMP_TIMEOUT,
        DEFAULT_TSS_TIMESTAMP_TIMEOUT);
    return new HTableSharedTimestampManager(tsTable, tsFamilyName, pool,
        tsTimeout);
  }

  public HTableSharedTimestampManager(HTable tsTable, byte[] tsFamily,
      ScheduledExecutorService pool, long timestampTimeout) {
    super(tsTable, tsFamily, pool, timestampTimeout);
  }

  @Override
  public long acquireShared() throws IOException {
    return super.acquire();
  }

  // TODO could check if persisted
  @Override
  public boolean releaseShared(long ts) throws IOException {
    return super.release(ts);
  }

  @Override
  public long acquireReference(long ts) throws NoSuchTimestampException,
      IOException {
    return tsTable.incrementColumnValue(timestampRow, tsFamily,
        referenceCounter, 1L);
  }

  @Override
  public boolean releaseReference(long ts, long rid) throws IOException {
    while (true) {
      Get get = new Get(timestampRow);
      get.setTimeStamp(ts);
      get.addColumn(tsFamily, references);
      Result result = tsTable.get(get);
      byte[] value = result.getValue(tsFamily, references);
      if (value == null)
        return false;
      Collection<Long> refs = fromByteArray(value);
      if (!refs.remove(rid))
        return false;
      if (refs.isEmpty())
        return release(ts);
      byte[] newValue = toByteArray(refs);
      Put put = new Put(timestampRow, ts);
      put.add(tsFamily, references, newValue);
      if (tsTable.checkAndPut(timestampRow, tsFamily, references, value, put))
        return true;
      LOG.warn("concurrent reference release " + ts + ":" + rid);
    }
  }

  @Override
  public boolean isReferencePersisted(long ts, long rid)
      throws NoSuchTimestampException, IOException {
    Get get = new Get(timestampRow);
    get.setTimeStamp(ts);
    get.addColumn(tsFamily, references);
    Result result = tsTable.get(get);
    byte[] value = result.getValue(tsFamily, references);
    if (value == null)
      return false;
    return fromByteArray(value).contains(rid);
  }

  @Override
  public void persistReferences(long ts, Iterable<Long> rids)
      throws NotOwnerException, IOException {
    Put put = new Put(timestampRow, ts);
    byte[] refs = toByteArray(rids);
    put.add(tsFamily, references, refs);
    tsTable.put(put);
  }

  private byte[] toByteArray(Iterable<Long> rids) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    for (Long rid : rids)
      out.writeLong(rid);
    out.close();
    return out.getData();
  }

  private Collection<Long> fromByteArray(byte[] refs) throws IOException {
    Collection<Long> col = new ArrayList<Long>();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(refs, refs.length);
    while (true) {
      try {
        col.add(in.readLong());
      } catch (EOFException e) {
        break;
      }
    }
    in.close();
    return col;
  }

  @Override
  boolean isReclaimable(NavigableMap<byte[], byte[]> timestamp) {
    byte[] bytes = timestamp.get(releasedColumn);
    if (Bytes.toBoolean(bytes))
      return true;
    byte[] refs = timestamp.get(references);
    if (refs != null)
      return false;
    long time = Bytes.toLong(timestamp.get(timeColumn));
    long current = System.currentTimeMillis();
    if (current > time && (time + timestampTimeout) < current)
      return true;
    // otherwise it's still in use
    return false;
  }
}
