package edu.illinois.htx.tm.region;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.tm.log.XALog;

public class XAHRegionLog extends HRegionLog implements XALog<HKey, HLogRecord> {

  XAHRegionLog(HTable table, byte[] family, ExecutorService pool,
      HRegionInfo regionInfo) {
    super(table, family, pool, regionInfo);
  }

  @Override
  protected HLogRecord create(int type) {
    switch (type) {
    case XALog.RECORD_TYPE_JOIN:
      return new HJoinLogRecord();
    default:
      return super.create(type);
    }
  }

  @Override
  public long appendJoinLogRecord(long tid, long pid) throws IOException {
    return append(new HJoinLogRecord(nextSID(), tid, pid));
  }

}
