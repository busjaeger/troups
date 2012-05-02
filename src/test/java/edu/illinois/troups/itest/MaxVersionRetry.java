package edu.illinois.troups.itest;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import edu.illinois.troups.client.Get;
import edu.illinois.troups.client.HTable;
import edu.illinois.troups.client.Put;
import edu.illinois.troups.client.tm.Transaction;
import edu.illinois.troups.client.tm.TransactionManager;
import edu.illinois.troups.tm.region.HRegionTransactionManager;

public class MaxVersionRetry {

  private static final byte[] tableName = toBytes("maxversion");
  private static final byte[] familyName = toBytes("balance");
  private static final byte[] qualifierName = toBytes("main");
  private static final byte[] row = toBytes("10");

  private final HBaseAdmin admin;

  public MaxVersionRetry(HBaseAdmin admin) {
    this.admin = admin;
  }

  void start() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(familyName));
    desc.addCoprocessor(HRegionTransactionManager.class.getName());
    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      e.printStackTrace(System.out);
      // ignore
    }

    org.apache.hadoop.hbase.client.HTable t = new org.apache.hadoop.hbase.client.HTable(
        tableName);
    org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(
        toBytes(1L));
    put.add(familyName, qualifierName, Bytes.toBytes(1));
    t.put(put);
    t.close();
  }

  void stop() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  void run() throws IOException {
    Configuration conf = admin.getConfiguration();
    HTable table = new HTable(conf, tableName);
    TransactionManager tm = TransactionManager.get(conf);

    Put put = new Put(row);
    put.add(familyName, qualifierName, Bytes.toBytes(0));

    Get get = new Get(row);
    get.addColumn(familyName, qualifierName);

    Transaction ta = tm.begin();
    table.put(ta, put);
    ta.rollback();

    ta = tm.begin();
    table.put(ta, put);
    ta.rollback();

    ta = tm.begin();
    table.get(ta, get);
    ta.commit();
  }

  public static void main(String[] args) throws Exception,
      ZooKeeperConnectionException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    MaxVersionRetry rr = new MaxVersionRetry(admin);
    rr.start();
    try {
      rr.run();
    } finally {
      rr.stop();
    }
  }
}
