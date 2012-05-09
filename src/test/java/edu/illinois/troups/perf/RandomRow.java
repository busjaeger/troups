package edu.illinois.troups.perf;

import static edu.illinois.troups.util.perf.ThreadLocalStopWatch.start;
import static edu.illinois.troups.util.perf.ThreadLocalStopWatch.stop;
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

import edu.illinois.troups.util.perf.ThreadLocalStopWatch;
import edu.illinois.troups.util.perf.Times;

public class RandomRow {

  private static final byte[] familyName = toBytes("balance");
  private static final byte[] qualifierName = toBytes("main");

  private final HBaseAdmin admin;
  private final int num;
  private final byte[] tableName;

  public RandomRow(HBaseAdmin admin, int num, String tableName) {
    this.admin = admin;
    this.num = num;
    this.tableName = Bytes.toBytes(tableName);
  }

  void before() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(familyName));

    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      e.printStackTrace(System.out);
      // ignore
    }
  }

  void after() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  void run() throws IOException {
    HTable table = new HTable(admin.getConfiguration(), tableName);
    Random rand = new Random();

    long before = System.currentTimeMillis();
    Times times = new Times();
    for (int i = 0; i < num; i++) {
      long rowID = rand.nextLong();
      byte[] row = Bytes.toBytes(rowID);

      ThreadLocalStopWatch.start(times);
      try {
        start("get");
        try {
          Get get = new Get(row);
          get.addColumn(familyName, qualifierName);
          table.get(get);
        } finally {
          stop();
        }

        start("put");
        try {
          Put put = new Put(row);
          put.add(familyName, qualifierName, new byte[1024]);
          table.put(put);
        } finally {
          stop();
        }
      } finally {
        ThreadLocalStopWatch.stop(times);
      }

      if (i % 100 == 0)
        System.out.println("100 times");
    }
    long total = System.currentTimeMillis() - before;
    System.out.println("ran "+num+" transactions in "+total+" milliseconds");
    times.write(System.out);
  }

  public static void main(String[] args) throws Exception,
      ZooKeeperConnectionException {
    int repititions = 100;
    String tableName = "account";
    if (args.length > 0)
      tableName = args[0];
    if (args.length > 1)
      repititions = Integer.parseInt(args[1]);

    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    RandomRow rr = new RandomRow(admin, repititions, tableName);
    rr.before();
    try {
      rr.run();
    } finally {
      rr.after();
    }
  }
}
