package edu.illinois.htx.regionserver;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterables;

import edu.illinois.htx.HTXConstants;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.mvto.MVTOTransactionManager;

/**
 * TODO (in order of priority):
 * <ol>
 * <li>Implement internal scanner to delete unused versions during compaction
 * <li>orderly server shutdown
 * <li>recover from server crashes
 * <li>support regions splits
 * <li>add distributed transactions
 * </ol>
 */
public class HRegionTransactionManager extends BaseRegionObserver implements
    HRegionTransactionManagerProtocol {

  private MVTOTransactionManager<HKey> tm;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    HRegion region = ((RegionCoprocessorEnvironment) e).getRegion();
    HRegionKeyValueStore kvs = new HRegionKeyValueStore(region);
    int count = e.getConfiguration().getInt(HTXConstants.TM_THREAD_COUNT,
        HTXConstants.DEFAULT_TSO_HANDLER_COUNT);
    ScheduledExecutorService ex = Executors.newScheduledThreadPool(count);
    HRegionTransactionLog tlog = new HRegionTransactionLog();
    this.tm = new MVTOTransactionManager<HKey>(kvs, ex, tlog);
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    this.tm.getExecutorService().shutdown();
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    Long tid = getTID(put);
    if (tid == null)
      return;
    if (getBoolean(put, HTXConstants.ATTR_NAME_BEG))
      tm.begin(tid);
    boolean isDelete = getBoolean(put, HTXConstants.ATTR_NAME_DEL);
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue())
        tm.preWrite(tid, new HKey(kv), isDelete);
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    Long tid = getTID(put);
    if (tid == null)
      return;
    boolean isDelete = getBoolean(put, HTXConstants.ATTR_NAME_DEL);
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue())
        tm.postWrite(tid, new HKey(kv), isDelete);
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<KeyValue> results) throws IOException {
    Long tid = getTID(get);
    if (tid == null)
      return;
    if (getBoolean(get, HTXConstants.ATTR_NAME_BEG))
      tm.begin(tid);
    // TODO check if results are already sorted by HBase; and verify newer
    // versions are sorted before older versions by Comparator
    Collections.sort(results, KeyValue.COMPARATOR);
    Iterable<HKeyVersion> it = Iterables.transform(results,
        HKeyVersion.KEYVALUE_TO_KEYVERSION);
    tm.filterReads(tid, it);
  }

  private static Long getTID(OperationWithAttributes operation) {
    byte[] tsBytes = operation.getAttribute(HTXConstants.ATTR_NAME_TID);
    return tsBytes == null ? null : Bytes.toLong(tsBytes);
  }

  private static boolean getBoolean(OperationWithAttributes operation,
      String name) {
    byte[] bytes = operation.getAttribute(name);
    return bytes == null ? false : Bytes.toBoolean(bytes);
  }

  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> e, Store store,
      InternalScanner scanner) {
    if (e.getEnvironment().getRegion().getRegionInfo().isMetaTable()) {
      return scanner;
    } else {
      return scanner;// TODO
    }
  }

  @Override
  public void begin(long tid) {
    tm.begin(tid);
  }

  @Override
  public void commit(long tid) throws TransactionAbortedException {
    tm.commit(tid);
  }

  @Override
  public void abort(long tid) {
    tm.abort(tid);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return HRegionTransactionManagerProtocol.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }

}
