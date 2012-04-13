package edu.illinois.htx.tm.mvto;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.illinois.htx.test.InMemoryTransactionLog;
import edu.illinois.htx.test.SequentialExecutorService;
import edu.illinois.htx.test.StringKey;
import edu.illinois.htx.test.StringKeyValueStore;
import edu.illinois.htx.test.StringKeyVersion;
import edu.illinois.htx.tm.TransactionAbortedException;

public class MVTOTransactionManagerTest {

  private MVTOTransactionManager<StringKey> tm;
  private StringKeyValueStore kvs;
  private SequentialExecutorService ses;
  private InMemoryTransactionLog log;

  @Before
  public void before() {
    kvs = new StringKeyValueStore();
    ses = new SequentialExecutorService();
    log = new InMemoryTransactionLog();
    tm = new MVTOTransactionManager<StringKey>(kvs, ses, log);
    tm.start();
  }

  /**
   * scenario: transaction 0 has written version 0 to key x
   * 
   */
  @Test
  public void testWriteConflict() throws TransactionAbortedException {
    // state in the data store
    StringKey key = new StringKey("x");
    Iterable<StringKey> keys = Arrays.asList(key);
    long version = 0;
    kvs.writeVersion(key, version);

    tm.begin(1);
    tm.begin(2);

    // both transactions read the initial version
    Iterable<StringKeyVersion> versions = kvs.readVersions(key);
    tm.filterReads(1, versions);
    tm.filterReads(2, versions);

    kvs.writeVersion(key, 1);
    try {
      tm.preWrite(1, false, keys);
      Assert.fail("transaction 1 should have failed write check");
    } catch (TransactionAbortedException e) {
      // expected
    }

    kvs.writeVersion(key, 2);
    try {
      tm.preWrite(2, false, keys);
    } catch (TransactionAbortedException e) {
      e.printStackTrace();
      Assert.fail("tran 2 aborted unexpectedly");
    }

    ses.executedScheduledTasks();

    // at this point we expect only version 2 of x to be present
    versions = kvs.readVersions(key);
    Iterator<StringKeyVersion> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 0), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 2), it.next());
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testReadConflict() throws TransactionAbortedException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = 0;
    Iterable<StringKey> keys = Arrays.asList(key);
    kvs.writeVersion(key, version);

    tm.begin(1);
    tm.begin(2);

    Iterable<StringKeyVersion> versions = kvs.readVersions(key);
    tm.filterReads(1, versions);
    tm.preWrite(1, false, keys);

    /*
     * transaction 2 executes a read AFTER we have admitted the write, but
     * BEFORE the write is applied, so it does not see the written version. It
     * has to be aborted, because if we let it read the initial version of x,
     * the schedule is no longer serializable.
     */
    try {
      tm.filterReads(2, versions);
      Assert.fail("read should not be permitted");
    } catch (TransactionAbortedException e) {
      // expected
    }
    kvs.writeVersion(key, 1);
    tm.postWrite(1, false, keys);
    tm.commit(1);

    // at this point we expect only version 2 of x to be present
    versions = kvs.readVersions(key);
    Iterator<StringKeyVersion> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 0), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 1), it.next());
    Assert.assertFalse(it.hasNext());
  }

  /**
   * tests that a blocked transaction can be committed after TM restart
   */
  @Test
  public void testRestart() throws TransactionAbortedException,
      InterruptedException, ExecutionException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = 0;
    Iterable<StringKey> keys = Arrays.asList(key);
    kvs.writeVersion(key, version);

    tm.begin(1);
    tm.begin(2);

    tm.filterReads(1, kvs.readVersions(key));
    tm.preWrite(1, false, keys);
    kvs.writeVersion(key, 1);
    tm.postWrite(1, false, keys);

    tm.filterReads(2, kvs.readVersions(key));
    tm.preWrite(2, false, keys);
    kvs.writeVersion(key, 2);
    tm.postWrite(2, false, keys);

    Callable<Void> commit2 = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        tm.commit(2);
        return null;
      }
    };
    ExecutorService es = Executors.newFixedThreadPool(1);
    Future<Void> f = es.submit(commit2);

    Thread.sleep(100);
    tm.stop();

    try {
      f.get();
      Assert.fail("expected exception");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
    }

    tm = new MVTOTransactionManager<StringKey>(kvs, ses, log);
    tm.start();
    f = es.submit(commit2);
    tm.commit(1);
    try {
      f.get();
    } catch (ExecutionException e) {
      throw e;
    }

    // at this point we expect only version 2 of x to be present
    Iterable<StringKeyVersion> versions = kvs.readVersions(key);
    Iterator<StringKeyVersion> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 0), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 1), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(new StringKeyVersion(key, 2), it.next());
    Assert.assertFalse(it.hasNext());
  }
}