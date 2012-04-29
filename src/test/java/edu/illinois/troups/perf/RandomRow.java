package edu.illinois.troups.perf;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RandomRow {

  private static final byte[] tableName = toBytes("account");
  private static final byte[] familyName = toBytes("balance");
  private static final byte[] qualifierName = toBytes("main");

  private final HBaseAdmin admin;

  public RandomRow(HBaseAdmin admin) {
    this.admin = admin;
  }

  void start() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(familyName));

    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      e.printStackTrace();
      // ignore
    }
  }

  void stop() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  void run() throws IOException {
    HTable table = new HTable(admin.getConfiguration(), tableName);
    Random rand = new Random();

    long gett = 0, putt = 0, tt = 0;
    int num = 1000;
    long before, beforeOp;
    for (int i = 0; i < num; i++) {
      long rowID = rand.nextLong();
      byte[] row = Bytes.toBytes(rowID);

      before = System.nanoTime();

      beforeOp = System.nanoTime();
      Get get = new Get(row);
      get.addColumn(familyName, qualifierName);
      table.get(get);
      gett += (System.nanoTime() - beforeOp);

      beforeOp = System.nanoTime();
      Put put = new Put(row);
      put.add(familyName, qualifierName, new byte[1024]);
      table.put(put);
      putt += (System.nanoTime() - beforeOp);

      tt += (System.nanoTime() - before);

      if (i % 100 == 0)
        System.out.println("100 times");
    }
    System.out.println("Average total: " + (tt / num));
    System.out.println("Average get: " + (gett / num));
    System.out.println("Average put: " + (putt / num));
  }

  public static void main(String[] args) throws Exception,
      ZooKeeperConnectionException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    RandomRow rr = new RandomRow(admin);
    rr.start();
    try {
      rr.run();
    } finally {
      rr.stop();
    }
  }
}
