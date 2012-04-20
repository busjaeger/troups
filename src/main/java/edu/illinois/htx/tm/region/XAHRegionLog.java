package edu.illinois.htx.tm.region;

import static edu.illinois.htx.Constants.DEFAULT_TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.htx.Constants.DEFAULT_TM_LOG_TABLE_NAME;
import static edu.illinois.htx.Constants.TM_LOG_TABLE_FAMILY_NAME;
import static edu.illinois.htx.Constants.TM_LOG_TABLE_NAME;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.tm.XATransactionState;
import edu.illinois.htx.tm.XID;
import edu.illinois.htx.tm.log.XALog;
import edu.illinois.htx.tm.log.XAStateTransitionLogRecord;

public class XAHRegionLog extends HRegionLog implements XALog<HKey, HLogRecord> {

  public static XAHRegionLog newInstance(HConnection connection,
      ExecutorService pool, HRegionInfo regionInfo) throws IOException {
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
    return new XAHRegionLog(table, family, pool, regionInfo);
  }

  XAHRegionLog(HTable table, byte[] family, ExecutorService pool,
      HRegionInfo regionInfo) {
    super(table, family, pool, regionInfo);
  }

  @Override
  protected HLogRecord create(int type) {
    switch (type) {
    case XALog.RECORD_TYPE_XA_STATE_TRANSITION:
      return new HXAStateTransitionLogRecord();
    default:
      return super.create(type);
    }
  }

  @Override
  protected boolean isStarted(HLogRecord record) {
    return record.getType() == XALog.RECORD_TYPE_XA_STATE_TRANSITION
        && ((XAStateTransitionLogRecord) record).getTransactionState() == XATransactionState.JOINED;
  }

  @Override
  public long appendXAStateTransition(XID xid, int state) throws IOException {
    return append(new HXAStateTransitionLogRecord(nextSID(), xid, state));
  }

}
