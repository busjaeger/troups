package edu.illinois.troup.tm.region;

import static edu.illinois.troup.Constants.DEFAULT_TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.troup.Constants.DEFAULT_TM_LOG_TABLE_NAME;
import static edu.illinois.troup.Constants.TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.troup.Constants.TM_LOG_TABLE_NAME;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;

import edu.illinois.troup.tm.XID;
import edu.illinois.troup.tm.log.CrossGroupLog;

public class HCrossGroupRegionLog extends HRegionLog implements
    CrossGroupLog<HKey, HLogRecord> {

  public static HCrossGroupRegionLog newInstance(HConnection connection,
      ExecutorService pool, HRegion region) throws IOException {
    Configuration conf = connection.getConfiguration();
    byte[] tableName = toBytes(conf.get(TM_LOG_TABLE_NAME,
        DEFAULT_TM_LOG_TABLE_NAME));
    byte[] family = toBytes(conf.get(TM_LOG_TABLE_FAMILY_NAME,
        DEFAULT_TM_LOG_TABLE_FAMILY_NAME));
    // create log table if necessary
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(tableName)) {
      HTableDescriptor descr = new HTableDescriptor(tableName);
      descr.addFamily(new HColumnDescriptor(family));
      try {
        admin.createTable(descr);
      } catch (TableExistsException e) {
        // ignore: concurrent creation
      }
    }
    HTable table = new HTable(tableName, connection, pool);
    return new HCrossGroupRegionLog(table, family, pool, region);
  }

  HCrossGroupRegionLog(HTable table, byte[] family, ExecutorService pool,
      HRegion region) {
    super(table, family, pool, region);
  }

  @Override
  protected HLogRecord create(int type) {
    switch (type) {
    case CrossGroupLog.RECORD_TYPE_XG_STATE_TRANSITION:
      return new HCrossGroupStateTransitionLogRecord();
    default:
      return super.create(type);
    }
  }

  @Override
  public long appendCrossGroupStateTransition(XID xid, HKey groupKey, int state)
      throws IOException {
    HLogRecord record = new HCrossGroupStateTransitionLogRecord(nextSID(), xid,
        groupKey, state);
    return append(record);
  }

}
