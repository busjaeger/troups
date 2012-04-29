package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.Constants.DEFAULT_LOG_IMPL;
import static edu.illinois.troups.Constants.DEFAULT_LOG_TABLE_NAME;
import static edu.illinois.troups.Constants.DEFAULT_TM_THREAD_COUNT;
import static edu.illinois.troups.Constants.DEFAULT_TRANSACTION_TIMEOUT;
import static edu.illinois.troups.Constants.LOG_IMPL;
import static edu.illinois.troups.Constants.LOG_TABLE_NAME;
import static edu.illinois.troups.Constants.TM_THREAD_COUNT;
import static edu.illinois.troups.Constants.TRANSACTION_TIMEOUT;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.illinois.troups.Constants;
import edu.illinois.troups.client.tm.RowGroupPolicy;
import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.XATransactionLog;
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.impl.MVTOTransactionManager;
import edu.illinois.troups.tm.impl.MVTOXATransactionManager;
import edu.illinois.troups.tmg.CrossGroupTransactionManager;
import edu.illinois.troups.tmg.GroupTransactionManager;
import edu.illinois.troups.tsm.TimestampManager.TimestampReclamationListener;
import edu.illinois.troups.tsm.zk.TimestampReclaimer;
import edu.illinois.troups.tsm.zk.ZKSharedTimestampManager;

public class HRegionTransactionManager extends BaseRegionObserver implements
    GroupTransactionManager<HKey>, CrossGroupTransactionManager<HKey>,
    TimestampReclamationListener, KeyValueStore<HKey> {

  public static final Log LOG = LogFactory.getLog(HRegion.class);

  static final Comparator<KeyValue> COMP = KeyValue.COMPARATOR
      .getComparatorIgnoringTimestamps();

  private final ConcurrentMap<HKey, MVTOXATransactionManager<HKey, HRecord>> tms = new ConcurrentHashMap<HKey, MVTOXATransactionManager<HKey, HRecord>>();
  private boolean started = false;
  private HRegion region;
  private RowGroupPolicy groupPolicy;
  private GroupLogStore logStore;
  private ScheduledExecutorService pool;
  private long transactionTimeout;

  private ZKSharedTimestampManager tsm;

  private TimestampReclaimer collector;
  private volatile long lrt;

  // temporary to measure response time
  private long beginN;
  private long beginT;
  private long preGetN;
  private long preGetT;
  private long postGetN;
  private long postGetT;
  private long prePutN;
  private long prePutT;
  private long postPutN;
  private long postPutT;
  private long commitN;
  private long commitT;

  private MVTOXATransactionManager<HKey, HRecord> getTM(HKey groupKey) {
    MVTOXATransactionManager<HKey, HRecord> tm = tms.get(groupKey);
    if (tm == null)
      throw new IllegalStateException("No transaction started for group "
          + groupKey);
    return tm;
  }

  // TODO should also remove TMs when they are no longer used
  private MVTOXATransactionManager<HKey, HRecord> demandTM(HKey groupKey) {
    MVTOXATransactionManager<HKey, HRecord> tm = tms.get(groupKey);
    if (tm == null) {
      XATransactionLog<HKey, HRecord> logWrapper = new HCrossGroupTransactionLog(
          groupKey, logStore);
      tm = new MVTOXATransactionManager<HKey, HRecord>(this, logWrapper, tsm);
      MVTOXATransactionManager<HKey, HRecord> existing = tms.putIfAbsent(
          groupKey, tm);
      if (existing != null)
        tm = existing;
      else
        tm.start();
    }
    return tm;
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment env = ((RegionCoprocessorEnvironment) e);
    if (env.getRegion().getRegionInfo().isMetaTable())
      return;

    region = env.getRegion();
    groupPolicy = RowGroupSplitPolicy.getRowGroupStrategy(region);

    // create thread pool
    Configuration conf = env.getConfiguration();
    int count = conf.getInt(TM_THREAD_COUNT, DEFAULT_TM_THREAD_COUNT);
    pool = Executors.newScheduledThreadPool(count);

    transactionTimeout = conf.getLong(TRANSACTION_TIMEOUT,
        DEFAULT_TRANSACTION_TIMEOUT);

    // create time-stamp manager
    ZooKeeperWatcher zkw = env.getRegionServerServices().getZooKeeper();
    tsm = new ZKSharedTimestampManager(zkw);
    tsm.addTimestampReclamationListener(this);

    // create a log store
    String logImpl = conf.get(LOG_IMPL, DEFAULT_LOG_IMPL);
    if ("file".equals(logImpl)) {
      FileSystem fs = region.getFilesystem();
      Path groupsDir = new Path(region.getRegionDir().getParent(), "groups");
      if (!fs.exists(groupsDir))
        fs.mkdirs(groupsDir);
      logStore = new HSequenceFileLogStore(fs, groupsDir);
    } else {
      HConnection connection = env.getRegionServerServices()
          .getCatalogTracker().getConnection();
      HTable logTable = demandLogTable(connection, pool);
      byte[] tableName = region.getTableDesc().getName();
      logStore = new HTableLogStore(logTable, tableName);
    }

    // create timestamp collector
    collector = new TimestampReclaimer(tsm, conf, pool, zkw);
    started = true;
  }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
    if (!started)
      return;
    tsm.start();
    collector.start();
    // run transaction timeout thread every 5 seconds
    pool.scheduleAtFixedRate(new Runnable() {
      public void run() {
        for (MVTOTransactionManager<HKey, HRecord> tm : tms.values())
          tm.timeout(transactionTimeout);
      }
    }, 0, 5, TimeUnit.SECONDS);
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    // TODO start up transaction managers?
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    if (!started)
      return;

    LOG.info("begin time: " + average(beginT, beginN));
    LOG.info("preGet time: " + average(preGetT, preGetN));
    LOG.info("postGet time: " + average(postGetT, postGetN));
    LOG.info("prePut time: " + average(prePutT, prePutN));
    LOG.info("postPut time: " + average(postPutT, postPutN));
    LOG.info("commit time " + average(commitT, commitN));

    for (MVTOXATransactionManager<HKey, HRecord> tm : tms.values())
      tm.stop();

    // TODO graceful shutdown
    pool.shutdown();
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    for (MVTOXATransactionManager<HKey, HRecord> tm : tms.values())
      tm.stopped();
    super.postClose(e, abortRequested);
  }

  long average(long time, long num) {
    return num > 0 ? time / num : 0;
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    TID tid = getTID(put);
    if (tid == null)
      return;
    long before = System.currentTimeMillis();
    try {
      if (put.getTimeStamp() != tid.getTS())
        throw new IllegalArgumentException("timestamp does not match tid");
      boolean isDelete = getBoolean(put, Constants.ATTR_NAME_DEL);
      // create an HKey set view on the family map
      HKey groupKey = getGroupKey(put.getRow());
      Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
          .getFamilyMap().values(), HRegionTransactionManager
          .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
      if (isDelete)
        getTM(groupKey).beforeDelete(tid, keys);
      else
        getTM(groupKey).beforePut(tid, keys);
    } finally {
      prePutN++;
      prePutT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    TID tid = getTID(put);
    if (tid == null)
      return;
    long before = System.currentTimeMillis();
    try {
      boolean isDelete = getBoolean(put, Constants.ATTR_NAME_DEL);
      HKey groupKey = getGroupKey(put.getRow());
      Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
          .getFamilyMap().values(), HRegionTransactionManager
          .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
      if (isDelete)
        getTM(groupKey).beforeDelete(tid, keys);
      else
        getTM(groupKey).beforePut(tid, keys);
    } finally {
      postPutN++;
      postPutT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    long before = System.currentTimeMillis();
    try {
      TimeRange tr = get.getTimeRange();
      if (tr.getMin() != 0L || tr.getMax() != tid.getTS())
        throw new IllegalArgumentException(
            "timerange does not match tid: (expected: "
                + new TimeRange(0L, tid.getTS()) + "), (actual: " + tr);
      HKey groupKey = getGroupKey(get.getRow());
      Iterable<HKey> keys = transform(get.getRow(), get.getFamilyMap());
      getTM(groupKey).beforeGet(tid, keys);
    } finally {
      preGetN++;
      preGetT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      final List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    long before = System.currentTimeMillis();
    try {
      // TODO check if results are already sorted by HBase; and verify newer
      // versions are sorted before older versions by Comparator
      Collections.sort(results, KeyValue.COMPARATOR);
      HKey groupKey = getGroupKey(get.getRow());
      Iterable<KeyVersions<HKey>> kvs = transform(results);
      getTM(groupKey).afterGet(tid, kvs);
    } finally {
      postGetN++;
      postGetT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) {
    if (e.getEnvironment().getRegion().getRegionInfo().isMetaTable()) {
      return scanner;
    } else {
      return new VersionCollector(scanner, lrt);
    }
  }

  @Override
  public void deleteVersion(HKey key, long version) throws IOException {
    Delete delete = new Delete(key.getRow());
    delete.deleteColumn(key.getFamily(), key.getQualifier(), version);
    region.delete(delete, null, true);
  }

  @Override
  public void deleteVersions(HKey key, long version) throws IOException {
    Delete delete = new Delete(key.getRow());
    delete.deleteColumn(key.getFamily(), key.getQualifier(), version);
    region.delete(delete, null, true);
  }

  @Override
  public TID begin(HKey groupKey) throws IOException {
    long before = System.currentTimeMillis();
    try {
      return demandTM(groupKey).begin();
    } finally {
      beginN++;
      beginT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public void commit(HKey groupKey, TID tid)
      throws TransactionAbortedException, IOException {
    long before = System.currentTimeMillis();
    try {
      getTM(groupKey).commit(tid);
    } finally {
      commitN++;
      commitT += System.currentTimeMillis() - before;
    }
  }

  @Override
  public void abort(HKey groupKey, TID tid) throws IOException {
    getTM(groupKey).abort(tid);
  }

  @Override
  public XID join(HKey groupKey, TID tid) throws IOException {
    return demandTM(groupKey).join(tid);
  }

  @Override
  public void prepare(HKey groupKey, XID xid) throws IOException {
    getTM(groupKey).prepare(xid);
  }

  @Override
  public void commit(HKey groupKey, XID xid, boolean onePhase)
      throws IOException {
    getTM(groupKey).commit(xid, onePhase);
  }

  @Override
  public void abort(HKey groupKey, XID xid) throws IOException {
    getTM(groupKey).abort(xid);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return CrossGroupTransactionManager.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }

  @Override
  public void reclaimed(long timestamp) {
    lrt = timestamp;
  }

  private static TID getTID(OperationWithAttributes operation) {
    byte[] tidBytes = operation.getAttribute(Constants.ATTR_NAME_TID);
    if (tidBytes != null)
      return new TID(tidBytes);
    tidBytes = operation.getAttribute(Constants.ATTR_NAME_XID);
    if (tidBytes != null)
      return new XID(tidBytes);
    return null;
  }

  private static boolean getBoolean(OperationWithAttributes operation,
      String name) {
    byte[] bytes = operation.getAttribute(name);
    return bytes == null ? false : Bytes.toBoolean(bytes);
  }

  static <F, T> Function<? super Iterable<F>, ? extends Iterable<T>> map(
      final Function<? super F, ? extends T> function) {
    return new Function<Iterable<F>, Iterable<T>>() {
      @Override
      public Iterable<T> apply(Iterable<F> it) {
        return Iterables.transform(it, function);
      }
    };
  }

  // TODO make more general - written with assumptions about how it's used
  static Iterable<KeyVersions<HKey>> transform(final Iterable<KeyValue> kvs) {
    return new Iterable<KeyVersions<HKey>>() {
      @Override
      public Iterator<KeyVersions<HKey>> iterator() {
        return new Iterator<KeyVersions<HKey>>() {
          private final Iterator<KeyValue> it = kvs.iterator();

          private KeyValue next;

          @Override
          public boolean hasNext() {
            return next != null || it.hasNext();
          }

          @Override
          public KeyVersions<HKey> next() {
            final KeyValue first;
            if (next == null)
              first = it.next();
            else {
              first = next;
              next = null;
            }
            return new KeyVersions<HKey>() {
              @Override
              public HKey getKey() {
                return new HKey(first);
              }

              @Override
              public Iterable<Long> getVersions() {
                return new Iterable<Long>() {
                  @Override
                  public Iterator<Long> iterator() {
                    return new Iterator<Long>() {
                      boolean isFirst = true;

                      @Override
                      public boolean hasNext() {
                        if (isFirst)
                          return true;
                        if (!it.hasNext())
                          return false;
                        if (next == null)
                          next = it.next();
                        if (COMP.compare(next, first) != 0)
                          return false;
                        return true;
                      }

                      @Override
                      public Long next() {
                        long ts;
                        if (isFirst) {
                          ts = first.getTimestamp();
                          isFirst = false;
                        } else {
                          if (next == null) {
                            next = it.next();
                            if (COMP.compare(next, first) != 0)
                              throw new NoSuchElementException();
                          }
                          ts = next.getTimestamp();
                          next = null;
                        }
                        return ts;
                      }

                      @Override
                      public void remove() {
                        it.remove();
                      }
                    };
                  }
                };
              }
            };
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      };
    };
  }

  private Iterable<HKey> transform(final byte[] row,
      Map<byte[], NavigableSet<byte[]>> familyMap) {
    return Iterables.concat(Iterables.transform(familyMap.entrySet(),
        new Function<Entry<byte[], NavigableSet<byte[]>>, Iterable<HKey>>() {
          @Override
          public Iterable<HKey> apply(
              final Entry<byte[], NavigableSet<byte[]>> entry) {
            return Iterables.transform(entry.getValue(),
                new Function<byte[], HKey>() {
                  @Override
                  public HKey apply(byte[] qualifier) {
                    return new HKey(row, entry.getKey(), qualifier);
                  }
                });
          }
        }));
  }

  private HKey getGroupKey(byte[] row) {
    return new HKey(groupPolicy == null ? row : groupPolicy.getGroupKey(row));
  }

  private static HTable demandLogTable(HConnection connection,
      ExecutorService pool) throws IOException {
    Configuration conf = connection.getConfiguration();
    byte[] tableName = toBytes(conf.get(LOG_TABLE_NAME, DEFAULT_LOG_TABLE_NAME));
    byte[] familyName = toBytes(conf
        .get(LOG_TABLE_NAME, DEFAULT_LOG_TABLE_NAME));
    // create log table if necessary
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(tableName)) {
      HTableDescriptor descr = new HTableDescriptor(tableName);
      descr.addFamily(new HColumnDescriptor(familyName));
      try {
        admin.createTable(descr);
      } catch (TableExistsException e) {
        // ignore: concurrent creation
      }
    }
    return new HTable(tableName, connection, pool);
  }
}
