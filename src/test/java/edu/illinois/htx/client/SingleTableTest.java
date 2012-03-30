package edu.illinois.htx.client;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import edu.illinois.htx.tm.TransactionAbortedException;

public class SingleTableTest {

  @Ignore
  @Test
  public void test() throws IOException {
    byte[] row1 = toBytes("1");
    byte[] row2 = toBytes("2");
    byte[] family = toBytes("balance");
    byte[] qualifier = toBytes("main");

    Configuration conf = HBaseConfiguration.create();
    TransactionManager tm = new TransactionManager(conf);
    HTXTable table = new HTXTable(conf, "account");

    Transaction ta = tm.begin();
    try {
      Put put = new Put(row1).add(family, qualifier, toBytes(400L));
      table.put(ta, put);
      Put put2 = new Put(row2).add(family, qualifier, toBytes(600L));
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

    ta = tm.begin();
    try {
      // read balance of first account
      Get get1 = new Get(row1).addColumn(family, qualifier);
      Result result1 = table.get(ta, get1);
      long value1 = Bytes.toLong(result1.getValue(family, qualifier));

      // read balance of second account
      Get get2 = new Get(row2).addColumn(family, qualifier);
      Result result2 = table.get(ta, get2);
      long value2 = Bytes.toLong(result2.getValue(family, qualifier));

      // transfer money
      Put put = new Put(row1).add(family, qualifier, toBytes(value1 + 100));
      table.put(ta, put);
      Put put2 = new Put(row2).add(family, qualifier, toBytes(value2 - 100));
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
  }

  public static void main(String[] args) throws IOException {
    SingleTableTest test = new SingleTableTest();
    test.test();
    System.out.println("succeeded");
  }

}
