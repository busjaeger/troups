package edu.illinois.htx.regionserver;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
 * TODO:
 * <ol>
 * <li>add distributed transactions
 * <li>support regions splits
 * <li>support recovery
 * </ol>
 */
public class CoprocessorTransactionManager extends BaseRegionObserver implements
    CoprocessorTransactionManagerProtocol {

  private MVTOTransactionManager<HKey> tm;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    HRegion region = ((RegionCoprocessorEnvironment) e).getRegion();
    HRegionKeyValueStore kvs = new HRegionKeyValueStore(region);
    ExecutorService ex = Executors.newCachedThreadPool();
    this.tm = new MVTOTransactionManager<HKey>(kvs, ex);
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
    boolean isDelete = put.getAttribute(HTXConstants.ATTR_NAME_DEL) != null;
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue()) {
        HKey key = new HKey(kv);
        if (isDelete)
          tm.checkDelete(tid, key);
        else
          tm.checkWrite(tid, key);
      }
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<KeyValue> results) throws IOException {
    Long tts = getTID(get);
    if (tts == null)
      return;
    // TODO check if results are already sorted by HBase; and verify newer
    // versions are sorted before older versions by Comparator
    Collections.sort(results, KeyValue.COMPARATOR);
    tm.filterReads(tts, Iterables.transform(results, HKeyVersion.KEYVALUE_TO_KEYVERSION));
  }

  private static Long getTID(OperationWithAttributes operation) {
    byte[] tsBytes = operation.getAttribute(HTXConstants.ATTR_NAME_TID);
    return tsBytes == null ? null : Bytes.toLong(tsBytes);
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
    return CoprocessorTransactionManagerProtocol.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }

}
