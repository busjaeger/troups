package edu.illinois.htx.regionserver;

import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.VersionTracker;

public class HTXRegionObserver extends BaseRegionObserver {

  private VersionTracker tm;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    tm = null; // TODO connect to TM
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    // TODO disconnect TM
  }

  @Override
  public void postGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
      List<KeyValue> results) throws IOException {
    Long tts = getTransactionTimestamp(get);
    if (tts == null)
      return;
    byte[] table = e.getEnvironment().getRegion().getTableDesc().getName();
    for (Iterator<KeyValue> it = results.iterator(); it.hasNext();) {
      KeyValue kv = it.next();
      long v = tm.selectReadVersion(tts, new Key(table, kv));
      if (v != kv.getTimestamp())
        it.remove();
    }
  }

  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
      WALEdit edit, boolean writeToWAL) throws IOException {
    Long tts = getTransactionTimestamp(put);
    if (tts == null)
      return;
    boolean isDelete = put.getAttribute("htx-del") != null;
    byte[] table = e.getEnvironment().getRegion().getTableDesc().getName();
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue()) {
        Key key = new Key(table, kv);
        if (isDelete)
          tm.written(tts, key);
        else
          tm.deleted(tts, key);
      }
  }

  private static Long getTransactionTimestamp(OperationWithAttributes operation) {
    byte[] tsBytes = operation.getAttribute("htx-tts");
    return tsBytes == null ? null : Bytes.toLong(tsBytes);
  }

}
