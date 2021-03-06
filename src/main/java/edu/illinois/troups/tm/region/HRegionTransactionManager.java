package edu.illinois.troups.tm.region;

import static edu.illinois.troups.Constants.DEFAULT_LOG_IMPL;
import static edu.illinois.troups.Constants.DEFAULT_STOP_WATCH_ENABLED;
import static edu.illinois.troups.Constants.DEFAULT_TM_THREAD_COUNT;
import static edu.illinois.troups.Constants.DEFAULT_TRANSACTION_TIMEOUT;
import static edu.illinois.troups.Constants.DEFAULT_TSS_IMPL;
import static edu.illinois.troups.Constants.LOG_FAMILY_NAME;
import static edu.illinois.troups.Constants.LOG_IMPL;
import static edu.illinois.troups.Constants.LOG_IMPL_VALUE_FAMILY;
import static edu.illinois.troups.Constants.LOG_IMPL_VALUE_FILE;
import static edu.illinois.troups.Constants.LOG_IMPL_VALUE_TABLE;
import static edu.illinois.troups.Constants.STOP_WATCH_ENABLED;
import static edu.illinois.troups.Constants.TM_THREAD_COUNT;
import static edu.illinois.troups.Constants.TRANSACTION_TIMEOUT;
import static edu.illinois.troups.Constants.TSS_IMPL;
import static edu.illinois.troups.Constants.TSS_IMPL_VALUE_SERVER;
import static edu.illinois.troups.Constants.TSS_IMPL_VALUE_TABLE;
import static edu.illinois.troups.Constants.TSS_IMPL_VALUE_ZOOKEEPER;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
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
import edu.illinois.troups.tm.XID;
import edu.illinois.troups.tm.log.XATransactionLog;
import edu.illinois.troups.tm.mvto.MVTOTransactionManager;
import edu.illinois.troups.tm.mvto.MVTOXATransactionManager;
import edu.illinois.troups.tm.region.log.GroupLogStore;
import edu.illinois.troups.tm.region.log.HCrossGroupTransactionLog;
import edu.illinois.troups.tm.region.log.HRecord;
import edu.illinois.troups.tm.region.log.HRegionLogStore;
import edu.illinois.troups.tm.region.log.HSequenceFileLogStore;
import edu.illinois.troups.tm.region.log.HTableLogStore;
import edu.illinois.troups.tsm.SharedTimestampManager;
import edu.illinois.troups.tsm.TimestampManager.TimestampReclamationListener;
import edu.illinois.troups.tsm.TimestampReclaimer;
import edu.illinois.troups.tsm.server.TimestampManagerServerClient;
import edu.illinois.troups.tsm.table.HTableSharedTimestampManager;
import edu.illinois.troups.tsm.zk.ZKSharedTimestampManager;
import edu.illinois.troups.tsm.zk.ZKTimestampReclaimer;
import edu.illinois.troups.util.perf.ThreadLocalStopWatch;
import edu.illinois.troups.util.perf.Times;

public class HRegionTransactionManager extends BaseRegionObserver implements
    GroupTransactionManager<HKey>, CrossGroupTransactionManager<HKey>,
    TimestampReclamationListener, KeyValueStore<HKey> {

  public static final Log LOG = LogFactory.getLog(HRegion.class);

  private final ConcurrentMap<HKey, MVTOXATransactionManager<HKey, HRecord>> tms = new ConcurrentHashMap<HKey, MVTOXATransactionManager<HKey, HRecord>>();
  private boolean started = false;
  private HRegion region;
  private RowGroupPolicy groupPolicy;
  private GroupLogStore logStore;
  private ScheduledExecutorService pool;
  private HTablePool tablePool;
  private long transactionTimeout;
  private SharedTimestampManager tsm;
  private TimestampReclaimer collector;
  private volatile Long lastReclaimedTimestamp;

  // used to measure response time of different APIs
  private Times beginTimes;
  private Times commitTimes;
  private Times abortTimes;
  private Times joinTimes;
  private Times prepareTimes;
  private Times xaCommitTimes;
  private Times xaAbortTimes;
  private Times preGetTimes;
  private Times postGetTimes;
  private Times prePutTimes;
  private Times postPutTimes;

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
      tm = new MVTOXATransactionManager<HKey, HRecord>(this, logWrapper, tsm,
          pool);
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
    groupPolicy = RowGroupSplitPolicy.newInstance(region);

    // create thread pool
    Configuration conf = env.getConfiguration();
    int count = conf.getInt(TM_THREAD_COUNT, DEFAULT_TM_THREAD_COUNT);
    pool = Executors.newScheduledThreadPool(count);

    final HConnection connection = env.getRegionServerServices()
        .getCatalogTracker().getConnection();
    tablePool = new HTablePool(conf, Integer.MAX_VALUE,
        new HTableInterfaceFactory() {
          @Override
          public void releaseHTableInterface(HTableInterface table)
              throws IOException {
            table.close();
          }

          @Override
          public HTableInterface createHTableInterface(Configuration config,
              byte[] tableName) {
            try {
              return new HTable(tableName, connection, pool);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });

    transactionTimeout = conf.getLong(TRANSACTION_TIMEOUT,
        DEFAULT_TRANSACTION_TIMEOUT);

    // create time-stamp manager
    int tssImpl = conf.getInt(TSS_IMPL, DEFAULT_TSS_IMPL);
    switch (tssImpl) {
    case TSS_IMPL_VALUE_ZOOKEEPER:
      LOG.info("TSM: Zookeeper");
      ZooKeeperWatcher zkw = env.getRegionServerServices().getZooKeeper();
      tsm = new ZKSharedTimestampManager(zkw);
      Runnable r = new ZKTimestampReclaimer((ZKSharedTimestampManager) tsm);
      collector = new TimestampReclaimer(r, conf, pool, zkw);
      break;
    case TSS_IMPL_VALUE_TABLE:
      LOG.info("TSM: HTable");
      zkw = env.getRegionServerServices().getZooKeeper();
      tsm = HTableSharedTimestampManager.newInstance(conf, tablePool, pool);
      collector = new TimestampReclaimer((Runnable) tsm, conf, pool, zkw);
      break;
    case TSS_IMPL_VALUE_SERVER:
      LOG.info("TSM: Server");
      tsm = new TimestampManagerServerClient(conf, pool);
      break;
    }

    // create a log store
    int logImpl = conf.getInt(LOG_IMPL, DEFAULT_LOG_IMPL);
    switch (logImpl) {
    case LOG_IMPL_VALUE_TABLE:
      LOG.info("Log: HTable");
      byte[] tableName = region.getTableDesc().getName();
      logStore = HTableLogStore.newInstance(tablePool, conf, tableName);
      break;
    case LOG_IMPL_VALUE_FILE:
      LOG.info("Log: File");
      FileSystem fs = region.getFilesystem();
      Path groupsDir = new Path(region.getRegionDir().getParent(), "groups");
      if (!fs.exists(groupsDir))
        fs.mkdirs(groupsDir);
      logStore = new HSequenceFileLogStore(fs, groupsDir);
      break;
    case LOG_IMPL_VALUE_FAMILY:
      LOG.info("Log: Region");
      String logFamily = region.getTableDesc().getValue(LOG_FAMILY_NAME);
      if (logFamily == null)
        logFamily = Constants.DEFAULT_LOG_FAMILY_NAME;
      logStore = new HRegionLogStore(region, Bytes.toBytes(logFamily));
    }

    if (conf.getBoolean(STOP_WATCH_ENABLED, DEFAULT_STOP_WATCH_ENABLED)) {
      beginTimes = new Times();
      commitTimes = new Times();
      abortTimes = new Times();
      joinTimes = new Times();
      prepareTimes = new Times();
      xaCommitTimes = new Times();
      xaAbortTimes = new Times();
      preGetTimes = new Times();
      postGetTimes = new Times();
      prePutTimes = new Times();
      postPutTimes = new Times();
    }

    // create timestamp collector
    started = true;
  }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
    if (!started)
      return;
    if (collector != null)
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

    tsm.addTimestampReclamationListener(this);
    // TODO start up transaction managers?
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    if (!started)
      return;

    if (e.getEnvironment().getConfiguration()
        .getBoolean(STOP_WATCH_ENABLED, DEFAULT_STOP_WATCH_ENABLED)) {
      String logDir = System.getProperty("hbase.log.dir");
      if (logDir == null)
        logDir = "";
      try {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        String prefix = "times." + sdf.format(new Date()) + ".";
        preGetTimes.write(new File(logDir, prefix + "preGet.log"));
        postGetTimes.write(new File(logDir, prefix + "postGet.log"));
        prePutTimes.write(new File(logDir, prefix + "prePut.log"));
        postPutTimes.write(new File(logDir, prefix + "postPut.log"));
        beginTimes.write(new File(logDir, prefix + "begin.log"));
        commitTimes.write(new File(logDir, prefix + "commit.log"));
        abortTimes.write(new File(logDir, prefix + "abort.log"));
        joinTimes.write(new File(logDir, prefix + "join.log"));
        prepareTimes.write(new File(logDir, prefix + "prepare.log"));
        xaCommitTimes.write(new File(logDir, prefix + "xa-commit.log"));
        xaAbortTimes.write(new File(logDir, prefix + "xa-abort.log"));
      } catch (IOException ex) {
        LOG.error("failed to write measurements", ex);
        ex.printStackTrace(System.out);
      }
    }

    for (MVTOXATransactionManager<HKey, HRecord> tm : tms.values())
      tm.stopping();
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    for (MVTOXATransactionManager<HKey, HRecord> tm : tms.values())
      tm.stopped();
    pool.shutdown();
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
    ThreadLocalStopWatch.start(prePutTimes);
    try {
      if (put.getTimeStamp() != tid.getTS())
        throw new IllegalArgumentException("timestamp does not match tid");
      // create an HKey set view on the family map
      HKey groupKey = getGroupKey(put.getRow());
      Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
          .getFamilyMap().values(), HRegionTransactionManager
          .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
      getTM(groupKey).beforePut(tid, keys);
    } finally {
      ThreadLocalStopWatch.stop(prePutTimes);
    }
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    TID tid = getTID(put);
    if (tid == null)
      return;
    ThreadLocalStopWatch.start(postPutTimes);
    try {
      HKey groupKey = getGroupKey(put.getRow());
      Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
          .getFamilyMap().values(), HRegionTransactionManager
          .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
      getTM(groupKey).afterPut(tid, keys);
    } finally {
      ThreadLocalStopWatch.stop(postPutTimes);
    }
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    ThreadLocalStopWatch.start(preGetTimes);
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
      ThreadLocalStopWatch.stop(preGetTimes);
    }
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      final List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    ThreadLocalStopWatch.start(postGetTimes);
    try {
      // TODO check if results are already sorted by HBase; and verify newer
      // versions are sorted before older versions by Comparator
      // Collections.sort(results, KeyValue.COMPARATOR);
      HKey groupKey = getGroupKey(get.getRow());
      Iterable<KeyVersions<HKey>> kvs = transform(results);
      int numVersionsRetrieved = get.getMaxVersions();
      getTM(groupKey).afterGet(tid, numVersionsRetrieved, kvs);
    } finally {
      ThreadLocalStopWatch.stop(postGetTimes);
    }
  }

  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) {
    if (e.getEnvironment().getRegion().getRegionInfo().isMetaTable()
        || lastReclaimedTimestamp == null) {
      return scanner;
    } else {
      LOG.info("Creating collector with version " + lastReclaimedTimestamp);
      return new VersionCollector(tsm, scanner, lastReclaimedTimestamp);
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
    ThreadLocalStopWatch.start(beginTimes);
    try {
      return demandTM(groupKey).begin();
    } finally {
      ThreadLocalStopWatch.stop(beginTimes);
    }
  }

  @Override
  public void commit(HKey groupKey, TID tid)
      throws TransactionAbortedException, IOException {
    ThreadLocalStopWatch.start(commitTimes);
    try {
      getTM(groupKey).commit(tid);
    } finally {
      ThreadLocalStopWatch.stop(commitTimes);
    }
  }

  @Override
  public void abort(HKey groupKey, TID tid) throws IOException {
    ThreadLocalStopWatch.start(abortTimes);
    try {
      getTM(groupKey).abort(tid);
    } finally {
      ThreadLocalStopWatch.stop(abortTimes);
    }
  }

  @Override
  public XID join(HKey groupKey, TID tid) throws IOException {
    ThreadLocalStopWatch.start(joinTimes);
    try {
      return demandTM(groupKey).join(tid);
    } finally {
      ThreadLocalStopWatch.stop(joinTimes);
    }
  }

  @Override
  public void prepare(HKey groupKey, XID xid) throws IOException {
    ThreadLocalStopWatch.start(prepareTimes);
    try {
      getTM(groupKey).prepare(xid);
    } finally {
      ThreadLocalStopWatch.stop(postGetTimes);
    }
  }

  @Override
  public void commit(HKey groupKey, XID xid, boolean onePhase)
      throws IOException {
    ThreadLocalStopWatch.start(xaCommitTimes);
    try {
      getTM(groupKey).commit(xid, onePhase);
    } finally {
      ThreadLocalStopWatch.stop(xaCommitTimes);
    }
  }

  @Override
  public void abort(HKey groupKey, XID xid) throws IOException {
    ThreadLocalStopWatch.start(xaAbortTimes);
    try {
      getTM(groupKey).abort(xid);
    } finally {
      ThreadLocalStopWatch.stop(xaAbortTimes);
    }
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
    // find minimum reclaimable time stamp
    Long min = null;
    for (MVTOTransactionManager<?, ?> tm : tms.values()) {
      tm.reclaimed(timestamp);
      long lastReclaimed = tm.getLastReclaimed();
      if (min == null || min > lastReclaimed)
        min = lastReclaimed;
    }
    lastReclaimedTimestamp = min;
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

  static <F, T> Function<? super Iterable<F>, ? extends Iterable<T>> map(
      final Function<? super F, ? extends T> function) {
    return new Function<Iterable<F>, Iterable<T>>() {
      @Override
      public Iterable<T> apply(Iterable<F> it) {
        return Iterables.transform(it, function);
      }
    };
  }

  static Iterable<KeyVersions<HKey>> transform(final Iterable<KeyValue> kvs) {
    return new Iterable<KeyVersions<HKey>>() {
      @Override
      public Iterator<KeyVersions<HKey>> iterator() {
        return new Iterator<KeyVersions<HKey>>() {
          private final Iterator<KeyValue> it = kvs.iterator();

          private KeyValue current;
          private KeyValue next;

          private boolean advanceNext() {
            while (it.hasNext()) {
              next = it.next();
              if (!new HKey(next).equals(new HKey(current)))
                return true;
            }
            next = null;
            return false;
          }

          @Override
          public boolean hasNext() {
            if (current == null)
              return it.hasNext();
            if (next == null)
              return false;
            if (current == next)
              return advanceNext();
            return true;
          }

          @Override
          public KeyVersions<HKey> next() {
            if (current == null)
              next = it.next();
            else if (next == null)
              throw new NoSuchElementException();
            else if (current == next && !advanceNext())
              throw new NoSuchElementException();
            current = next;
            return new KeyVersions<HKey>() {
              final HKey key = new HKey(current);

              @Override
              public HKey getKey() {
                return key;
              }

              @Override
              public Iterable<Long> getVersions() {
                return new Iterable<Long>() {
                  @Override
                  public Iterator<Long> iterator() {
                    return new Iterator<Long>() {
                      private KeyValue currentVersion;
                      private KeyValue nextVersion;

                      private boolean advanceNext() {
                        if (it.hasNext()) {
                          nextVersion = it.next();
                          if (key.equals(new HKey(nextVersion)))
                            return true;
                          next = nextVersion;
                        }
                        return false;
                      }

                      @Override
                      public boolean hasNext() {
                        if (currentVersion == null)
                          return true;
                        if (currentVersion == nextVersion)
                          return advanceNext();
                        if (next == nextVersion)
                          return false;
                        return true;
                      }

                      @Override
                      public Long next() {
                        if (currentVersion == null) {
                          currentVersion = current;
                          nextVersion = current;
                          return currentVersion.getTimestamp();
                        }
                        if (nextVersion == next)
                          throw new NoSuchElementException();
                        if (currentVersion == nextVersion && !advanceNext())
                          throw new NoSuchElementException();
                        currentVersion = nextVersion;
                        return currentVersion.getTimestamp();
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

  public static void demandTable(Configuration conf, byte[] tableName,
      byte[] familyName) throws IOException {
    // create log table if necessary
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.tableExists(tableName)) {
        HTableDescriptor descr = new HTableDescriptor(tableName);
        descr.addFamily(new HColumnDescriptor(familyName));
        try {
          admin.createTable(descr);
        } catch (TableExistsException e) {
          // ignore: concurrent creation
        }
      }
    } finally {
      admin.close();
    }
  }

}
