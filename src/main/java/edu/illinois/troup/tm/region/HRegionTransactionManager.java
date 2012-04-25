package edu.illinois.troup.tm.region;

import static edu.illinois.troup.Constants.DEFAULT_TM_THREAD_COUNT;
import static edu.illinois.troup.Constants.TM_THREAD_COUNT;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
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

import edu.illinois.troup.Constants;
import edu.illinois.troup.client.tm.RowGroupPolicy;
import edu.illinois.troup.tm.GroupTransactionOperationObserver;
import edu.illinois.troup.tm.KeyValueStore;
import edu.illinois.troup.tm.KeyVersions;
import edu.illinois.troup.tm.LifecycleListener;
import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.TransactionAbortedException;
import edu.illinois.troup.tm.XID;
import edu.illinois.troup.tm.impl.MVTOCrossGroupTransactionManager;
import edu.illinois.troup.tsm.TimestampManager.TimestampReclamationListener;
import edu.illinois.troup.tsm.zk.TimestampReclaimer;
import edu.illinois.troup.tsm.zk.ZKSharedTimestampManager;

public class HRegionTransactionManager extends BaseRegionObserver implements
    RTM, TimestampReclamationListener, KeyValueStore<HKey> {

  static final Comparator<KeyValue> COMP = KeyValue.COMPARATOR
      .getComparatorIgnoringTimestamps();

  private final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();
  private final List<GroupTransactionOperationObserver<HKey>> observers = new CopyOnWriteArrayList<GroupTransactionOperationObserver<HKey>>();
  private boolean started = false;
  private HRegion region;
  private RowGroupPolicy groupPolicy;
  private MVTOCrossGroupTransactionManager<HKey, HLogRecord> tm;
  private ZKSharedTimestampManager tsm;
  private ScheduledExecutorService pool;
  private TimestampReclaimer collector;
  private volatile long lrt;

  // TODO think about startup/ownership
  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    RegionCoprocessorEnvironment env = ((RegionCoprocessorEnvironment) e);
    if (env.getRegion().getRegionInfo().isMetaTable())
      return;

    Configuration conf = env.getConfiguration();
    ZooKeeperWatcher zkw = env.getRegionServerServices().getZooKeeper();
    region = env.getRegion();
    groupPolicy = RowGroupSplitPolicy.getRowGroupStrategy(region);

    // create thread pool
    int count = conf.getInt(TM_THREAD_COUNT, DEFAULT_TM_THREAD_COUNT);
    pool = Executors.newScheduledThreadPool(count);

    // create time-stamp manager
    tsm = new ZKSharedTimestampManager(zkw);
    tsm.addTimestampReclamationListener(this);

    // create transaction manager
    HConnection connection = env.getRegionServerServices().getCatalogTracker()
        .getConnection();
    HCrossGroupRegionLog tlog = HCrossGroupRegionLog.newInstance(connection,
        pool, region);
    tm = new MVTOCrossGroupTransactionManager<HKey, HLogRecord>(this, tlog, tsm);

    // create timestamp collector
    collector = new TimestampReclaimer(tsm, conf, pool, zkw);
    started = true;
  }

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> ctx) {
    if (!started)
      return;
    for (LifecycleListener listener : lifecycleListeners)
      listener.starting();
    tsm.start();
    collector.start();
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> e,
      boolean abortRequested) {
    if (!started)
      return;
    for (LifecycleListener listener : lifecycleListeners)
      if (abortRequested)
        listener.aborting();
      else
        listener.stopping();
    pool.shutdown();
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    TID tid = getTID(put);
    if (tid == null)
      return;
    if (put.getTimeStamp() != tid.getTS())
      throw new IllegalArgumentException("timestamp does not match tid");
    boolean isDelete = getBoolean(put, Constants.ATTR_NAME_DEL);
    // create an HKey set view on the family map
    HKey groupKey = getGroupKey(put.getRow());
    Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
        .getFamilyMap().values(), HRegionTransactionManager
        .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
    if (isDelete)
      tm.beforeDelete(tid, groupKey, keys);
    else
      tm.beforePut(tid, groupKey, keys);
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    TID tid = getTID(put);
    if (tid == null)
      return;
    boolean isDelete = getBoolean(put, Constants.ATTR_NAME_DEL);
    HKey groupKey = getGroupKey(put.getRow());
    Iterable<HKey> keys = Iterables.concat(Iterables.transform(put
        .getFamilyMap().values(), HRegionTransactionManager
        .<KeyValue, HKey> map(HKey.KEYVALUE_TO_KEY)));
    if (isDelete)
      tm.beforeDelete(tid, groupKey, keys);
    else
      tm.beforePut(tid, groupKey, keys);
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    TimeRange tr = get.getTimeRange();
    if (tr.getMin() != 0L || tr.getMax() != tid.getTS())
      throw new IllegalArgumentException(
          "timerange does not match tid: (expected: "
              + new TimeRange(0L, tid.getTS()) + "), (actual: " + tr);
    HKey groupKey = getGroupKey(get.getRow());
    Iterable<HKey> keys = transform(get.getRow(), get.getFamilyMap());
    tm.beforeGet(tid, groupKey, keys);
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      final List<KeyValue> results) throws IOException {
    TID tid = getTID(get);
    if (tid == null)
      return;
    // TODO check if results are already sorted by HBase; and verify newer
    // versions are sorted before older versions by Comparator
    Collections.sort(results, KeyValue.COMPARATOR);
    HKey groupKey = getGroupKey(get.getRow());
    Iterable<KeyVersions<HKey>> kvs = transform(results);
    tm.afterGet(tid, groupKey, kvs);
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
  public void addTransactionOperationObserver(
      GroupTransactionOperationObserver<HKey> observer) {
    observers.add(observer);
  }

  @Override
  public void addLifecycleListener(LifecycleListener listener) {
    this.lifecycleListeners.add(listener);
  }

  @Override
  public TID begin(HKey groupKey) throws IOException {
    return tm.begin(groupKey);
  }

  @Override
  public void commit(TID tid) throws TransactionAbortedException, IOException {
    tm.commit(tid);
  }

  @Override
  public void abort(TID tid) throws IOException {
    tm.abort(tid);
  }

  @Override
  public XID join(TID tid, HKey groupKey) throws IOException {
    return tm.join(tid, groupKey);
  }

  @Override
  public void prepare(XID xid) throws IOException {
    tm.prepare(xid);
  }

  @Override
  public void commit(XID xid, boolean onePhase) throws IOException {
    tm.commit(xid, onePhase);
  }

  @Override
  public void abort(XID xid) throws IOException {
    tm.abort(xid);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return RTM.VERSION;
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

}
