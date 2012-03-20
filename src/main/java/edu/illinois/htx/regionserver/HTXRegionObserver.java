package edu.illinois.htx.regionserver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
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

import edu.illinois.htx.client.HTXConnection;
import edu.illinois.htx.client.HTXConnectionManager;
import edu.illinois.htx.tm.HTXConstants;
import edu.illinois.htx.tm.VersionTracker;

public class HTXRegionObserver extends BaseRegionObserver {

  private HTXConnection connection;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    Configuration conf = e.getConfiguration();
    this.connection = HTXConnectionManager.getConnection(conf);
    System.out.println("HTXRegionObserver started");
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    this.connection.close();
    System.out.println("HTXRegionObserver stopped");
  }

  VersionTracker getVersionTracker() throws IOException {
    return connection.getVersionTracker();
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
      long v = getVersionTracker().selectReadVersion(tts, table, kv.getRow(),
          kv.getFamily(), kv.getQualifier());
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
    boolean isDelete = put.getAttribute(HTXConstants.ATTR_NAME_DEL) != null;
    byte[] table = e.getEnvironment().getRegion().getTableDesc().getName();
    for (Entry<byte[], List<KeyValue>> entries : put.getFamilyMap().entrySet())
      for (KeyValue kv : entries.getValue())
        if (isDelete)
          connection.getVersionTracker().deleted(tts, table, kv.getRow(),
              kv.getFamily(), kv.getQualifier());
        else
          connection.getVersionTracker().written(tts, table, kv.getRow(),
              kv.getFamily(), kv.getQualifier());
  }

  private static Long getTransactionTimestamp(OperationWithAttributes operation) {
    byte[] tsBytes = operation.getAttribute(HTXConstants.ATTR_NAME_TTS);
    return tsBytes == null ? null : Bytes.toLong(tsBytes);
  }

}
