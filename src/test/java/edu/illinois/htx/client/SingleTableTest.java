package edu.illinois.htx.client;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import edu.illinois.htx.client.tm.Transaction;
import edu.illinois.htx.client.tm.TransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.region.HRegionTransactionManager;

public class SingleTableTest {

  private static final byte[] tableName = toBytes("account");
  private static final byte[] familyName = toBytes("balance");
  private static final byte[] qualifierName = toBytes("main");

  Configuration conf;
  HBaseAdmin admin;

  @Before
  public void before() throws IOException {
    this.conf = HBaseConfiguration.create();
    this.admin = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(familyName));
    desc.addCoprocessor(HRegionTransactionManager.class.getName());
    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      e.printStackTrace();
      // ignore
    }
    // create row to see if it makes a difference
    org.apache.hadoop.hbase.client.HTable t = new org.apache.hadoop.hbase.client.HTable(
        tableName);
    org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(
        toBytes("10"));
    put.add(familyName, qualifierName, Bytes.toBytes(1));
    t.put(put);
    t.close();
  }

  @Ignore
  @Test
  public void test() throws IOException {
    byte[] row1 = toBytes("1");
    byte[] row2 = toBytes("2");

    Configuration conf = HBaseConfiguration.create();
    TransactionManager tm = TransactionManager.get(conf);
    HTable table = new HTable(conf, tableName);

    Transaction ta = tm.begin();
    try {
      Put put = new Put(row1).add(familyName, qualifierName, toBytes(400L));
      table.put(ta, put);
      // Put put2 = new Put(row2).add(familyName, qualifierName, toBytes(600L));
      // table.put(ta, put2);
      ta.commit();
    } catch (TransactionAbortedException e) {
      throw e;
      // could retry here
    } catch (Exception e) {
      ta.rollback();
      throw new IOException(e);
      // could retry here
    }

    ta = tm.begin();
    try {
      // Put put = new Put(row1).add(familyName, qualifierName, toBytes(400L));
      // table.put(ta, put);
      Put put2 = new Put(row2).add(familyName, qualifierName, toBytes(600L));
      table.put(ta, put2);
      ta.commit();
    } catch (TransactionAbortedException e) {
      throw e;
      // could retry here
    } catch (Exception e) {
      ta.rollback();
      throw new IOException(e);
      // could retry here
    }

    ta = tm.beginXG();
    try {
      // read balance of first account
      Get get1 = new Get(row1).addColumn(familyName, qualifierName);
      Result result1 = table.get(ta, get1);
      long value1 = Bytes.toLong(result1.getValue(familyName, qualifierName));

      // read balance of second account
      Get get2 = new Get(row2).addColumn(familyName, qualifierName);
      Result result2 = table.get(ta, get2);
      long value2 = Bytes.toLong(result2.getValue(familyName, qualifierName));

      // transfer money
      Put put = new Put(row1).add(familyName, qualifierName,
          toBytes(value1 + 100));
      table.put(ta, put);
      Put put2 = new Put(row2).add(familyName, qualifierName,
          toBytes(value2 - 100));
      table.put(ta, put2);

      ta.commit();
    } catch (TransactionAbortedException e) {
      throw e;
      // could retry here
    } catch (Exception e) {
      ta.rollback();
      throw new IOException(e);
      // could retry here
    }

    table.close();
    tm.close();
    System.out.println("success");
  }

  @After
  public void after() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  public static void main(String[] args) throws IOException {
    SingleTableTest test = new SingleTableTest();
    test.before();
    try {
      test.test();
    } finally {
      test.after();
    }
    System.out.println("succeeded");
  }

}
