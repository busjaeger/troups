package edu.illinois.htx.regionserver;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.tm.HTXConstants;
import edu.illinois.htx.tm.TransactionManager;

/**
 * TODO:
 * <ol>
 * <li>add distributed transactions
 * <li>support regions splits
 * <li>pass region reference to TM so it can cleanup rows
 * </ol>
 */
public class HTXRegionObserver extends BaseRegionObserver {

  private TransactionManager tm;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    // TODO initialize TransactionManager (may need to overwrite other methods)
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    Long tts = getTransactionTimestamp(put);
    if (tts == null)
      return;
    boolean isDelete = put.getAttribute(HTXConstants.ATTR_NAME_DEL) != null;
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue())
        tm.write(tts, kv, isDelete);
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<KeyValue> results) throws IOException {
    Long tts = getTransactionTimestamp(get);
    if (tts == null)
      return;
    // TODO check if results are already sorted by HBase; and verify newer
    // versions are sorted before older versions by Comparator
    Collections.sort(results, KeyValue.COMPARATOR);
    tm.filterReads(tts, results);
  }

  private static Long getTransactionTimestamp(OperationWithAttributes operation) {
    byte[] tsBytes = operation.getAttribute(HTXConstants.ATTR_NAME_TTS);
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
}
