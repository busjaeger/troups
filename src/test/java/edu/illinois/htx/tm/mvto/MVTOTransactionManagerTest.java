package edu.illinois.htx.tm.mvto;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.illinois.htx.test.InMemoryTimestampManager;
import edu.illinois.htx.test.StringKey;
import edu.illinois.htx.test.StringKeyLog;
import edu.illinois.htx.test.StringKeyLogRecord;
import edu.illinois.htx.test.StringKeyValueStore;
import edu.illinois.htx.test.StringKeyVersions;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.TimestampManager;

public class MVTOTransactionManagerTest {

  private MVTOTransactionManager<StringKey, StringKeyLogRecord> tm;
  private StringKeyValueStore kvs;
  private StringKeyLog log;
  private TimestampManager tsm;

  @Before
  public void before() throws IOException {
    kvs = new StringKeyValueStore();
    log = new StringKeyLog();
    tsm = new InMemoryTimestampManager();
    tm = new MVTOTransactionManager<StringKey, StringKeyLogRecord>(kvs, log,
        tsm);
    kvs.addTransactionOperationObserver(tm);
    tm.start();
  }

  /**
   * scenario: transaction 0 has written version 0 to key x
   * 
   */
  @Test
  public void testWriteConflict() throws IOException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.create();
    kvs.putVersion(key, version);
    tsm.delete(version);

    long t1 = tm.begin();
    long t2 = tm.begin();

    // both transactions read the initial version
    kvs.getVersionsObserved(t1, key);
    kvs.getVersionsObserved(t2, key);

    try {
      kvs.putVersionObserved(t1, key);
      Assert.fail("transaction 1 should have failed write check");
    } catch (TransactionAbortedException e) {
      // expected
    }

    try {
      kvs.putVersionObserved(t2, key);
    } catch (TransactionAbortedException e) {
      e.printStackTrace();
      Assert.fail("tran 2 aborted unexpectedly");
    }
    tm.commit(t2);

    // at this point we expect only version 2 of x to be present
    Iterable<Long> versions = kvs.getVersions(key);
    Iterator<Long> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t2), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testReadConflict() throws IOException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.create();
    Iterable<StringKey> keys = Arrays.asList(key);
    kvs.putVersion(key, version);

    long t1 = tm.begin();
    long t2 = tm.begin();

    Iterable<Long> versions = kvs.getVersionsObserved(t1, key);
    tm.beforeWrite(t1, false, keys);

    /*
     * transaction 2 executes a read AFTER we have admitted the write, but
     * BEFORE the write is applied, so it does not see the written version. It
     * has to be aborted, because if we let it read the initial version of x,
     * the schedule is no longer serializable.
     */
    try {
      tm.afterRead(t2, singleton(key, versions));
      Assert.fail("read should not be permitted");
    } catch (TransactionAbortedException e) {
      // expected
    }
    kvs.putVersion(key, 1);
    tm.afterWrite(t1, false, keys);
    tm.commit(t1);

    // at this point we expect only version 2 of x to be present
    versions = kvs.getVersions(key);
    Iterator<Long> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t1), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  /**
   * tests that a blocked transaction can be committed after TM restart
   */
  @Test
  public void testRestart() throws IOException, InterruptedException,
      ExecutionException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.create();
    kvs.putVersion(key, version);

    final long t1 = tm.begin();
    final long t2 = tm.begin();

    kvs.getVersionsObserved(t1, key);
    kvs.putVersionObserved(t1, key);

    kvs.getVersionsObserved(t2, key);
    kvs.putVersionObserved(t2, key);

    Callable<Void> commit2 = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        tm.commit(t2);
        return null;
      }
    };
    ExecutorService es = Executors.newFixedThreadPool(1);
    Future<Void> f = es.submit(commit2);

    Thread.sleep(100);
    tm.stop(false);

    try {
      f.get();
      Assert.fail("expected exception");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
    }

    tm = new MVTOTransactionManager<StringKey, StringKeyLogRecord>(kvs, log,
        tsm);
    tm.start();
    f = es.submit(commit2);
    tm.commit(t1);
    try {
      f.get();
    } catch (ExecutionException e) {
      throw e;
    }

    // at this point we expect only version 2 of x to be present
    Iterable<Long> versions = kvs.getVersions(key);
    Iterator<Long> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t2), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t1), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  private static Iterable<StringKeyVersions> singleton(StringKey key,
      Iterable<Long> versions) {
    return Collections.singletonList(new StringKeyVersions(key, versions));
  }
}
