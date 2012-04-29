package edu.illinois.troups.itest;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import edu.illinois.troups.client.HTable;
import edu.illinois.troups.client.Put;
import edu.illinois.troups.client.tm.RowGroupPolicy;
import edu.illinois.troups.client.tm.Transaction;
import edu.illinois.troups.client.tm.TransactionManager;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tmg.impl.HRegionTransactionManager;

/**
 * This test case illustrates how two entities can be mapped to the same HBase
 * table using the pattern described in the Megastore paper. It also shows how
 * different instances of these entities can be grouped together and updated as
 * an atomic unit by using the transaction API.
 * <p>
 * Note that with basic HBase this would not be possible: regardless of whether
 * the entities are mapped into the same tables or not, modifications across
 * different rows are not atomic. That means if a failure occurs half way
 * through performing these operations, some changes may be applied while others
 * are not. In addition, HBase would not protect against interleaving updates
 * from other clients. For example, if another client tried to create the same
 * user, some rows from the first client may overwrite rows from the second
 * client. Without the transaction manager users would have to try to implement
 * some form of transaction logic themselves.
 */
public class RowGroupTest {

  private static final byte[] tableName = toBytes("user");

  private static final byte[] userName = toBytes("user.name");
  private static final byte[] first = toBytes("first");
  private static final byte[] last = toBytes("last");
  private static final byte[] userEmail = toBytes("user.email");
  private static final byte[] personal = toBytes("personal");

  private static final byte[] contactAddress = toBytes("contact.address");
  private static final byte[] zip = toBytes("zip");
  private static final byte[] street = toBytes("street");
  private static final byte[] contactPhone = toBytes("contact.phone");
  private static final byte[] primary = toBytes("primary");

  Configuration conf;
  HBaseAdmin admin;

  @Before
  public void before() throws IOException {
    this.conf = HBaseConfiguration.create();
    this.admin = new HBaseAdmin(conf);

    // 1. create a table that maps multiple tables into one using prefixes
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(userName));
    desc.addFamily(new HColumnDescriptor(userEmail));
    desc.addFamily(new HColumnDescriptor(contactAddress));
    desc.addFamily(new HColumnDescriptor(contactPhone));

    // 2. set the row group split policy (used to figure out which rows are in
    // the same group)
    desc.setValue("SPLIT_POLICY", RowGroupSplitPolicy.class.getName());
    desc.setValue(RowGroupPolicy.META_ATTR,
        CharDelimiterSplitPolicy.class.getName());

    // 3. configure the transaction manager coprocessor
    desc.addCoprocessor(HRegionTransactionManager.class.getName());

    // 4. create the table
    try {
      admin.createTable(desc);
    } catch (TableExistsException e) {
      e.printStackTrace();
      // ignore
    }

    // 5. work-around coprocessor exception by creating an initial row
    // TODO figure out proper fix so we can remove this
    org.apache.hadoop.hbase.client.HTable t = new org.apache.hadoop.hbase.client.HTable(
        tableName);
    org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(
        toBytes("10"));
    put.add(userName, first, Bytes.toBytes(1));
    t.put(put);
    t.close();
  }

  @Ignore
  @Test
  public void test() throws IOException {
    // 6. get the transaction manager
    TransactionManager tm = TransactionManager.get(conf);
    HTable table = new HTable(conf, tableName);

    // retry in case the transaction is aborted due to write conflicts
    for (int i = 0; i < 10; i++) {
      // 7. create a transaction > Note: we are creating a 'local' transaction
      Transaction ta = tm.begin();
      try {
        // 8. create the user row
        Put put = new Put(toBytes("1"));
        put.add(userName, first, toBytes("John"));
        put.add(userName, last, toBytes("Doe"));
        put.add(userEmail, personal, toBytes("john@doe.com"));
        table.put(ta, put);

        // 9. create a contact information row: note the row key prefix
        put = new Put(toBytes("1.1"));
        put.add(contactAddress, zip, toBytes("12345"));
        put.add(contactAddress, street, toBytes("campus drive"));
        put.add(contactPhone, primary, toBytes("1234567"));
        table.put(ta, put);

        // 10. create another contact information row
        put = new Put(toBytes("1.1"));
        put.add(contactAddress, zip, toBytes("12345"));
        put.add(contactAddress, street, toBytes("campus drive"));
        put.add(contactPhone, primary, toBytes("1234567"));
        table.put(ta, put);

        // 11. commit the changes
        ta.commit();
        break;
      } catch (TransactionAbortedException e) {
        // retry here
      } catch (Exception e) {
        // could retry here
        ta.rollback();
        throw new IOException(e);
        // could retry here
      }
    }

    // 12. close resources
    table.close();
    tm.close();
  }

  @After
  public void after() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  public static void main(String[] args) throws IOException {
    RowGroupTest test = new RowGroupTest();
    test.before();
    try {
      test.test();
    } finally {
      test.after();
    }
    System.out.println("succeeded");
  }
}
