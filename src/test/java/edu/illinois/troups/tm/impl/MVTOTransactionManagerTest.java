package edu.illinois.troups.tm.impl;

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
import org.junit.Ignore;
import org.junit.Test;

import edu.illinois.troups.test.StringKey;
import edu.illinois.troups.test.StringKeyLog;
import edu.illinois.troups.test.StringKeyLogRecord;
import edu.illinois.troups.test.StringKeyValueStore;
import edu.illinois.troups.test.StringKeyVersions;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.mvto.MVTOTransactionManager;
import edu.illinois.troups.tsm.TimestampManager;
import edu.illinois.troups.tsm.mem.InMemoryTimestampManager;

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
        tsm, null);// TODO fix
    kvs.addTransactionOperationObserver(tm);
    tm.start();
  }

  /**
   * scenario: transaction 0 has written version 0 to key x
   * 
   */
  @Ignore
  @Test
  public void testWriteConflict() throws IOException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.acquire();
    kvs.putVersion(key, version);
    tsm.release(version);

    TID t1 = tm.begin();
    TID t2 = tm.begin();

    // both transactions read the initial version
    kvs.getVersions(t1, key);
    kvs.getVersions(t2, key);

    try {
      kvs.putVersion(t1, key);
      Assert.fail("transaction 1 should have failed write check");
    } catch (TransactionAbortedException e) {
      // expected
    }

    try {
      kvs.putVersion(t2, key);
    } catch (TransactionAbortedException e) {
      e.printStackTrace(System.out);
      Assert.fail("tran 2 aborted unexpectedly");
    }
    tm.commit(t2);

    // at this point we expect only version 2 of x to be present
    Iterable<Long> versions = kvs.getVersions(key);
    Iterator<Long> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t2.getTS()), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  @Ignore
  @Test
  public void testReadConflict() throws IOException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.acquire();
    Iterable<StringKey> keys = Arrays.asList(key);
    kvs.putVersion(key, version);

    TID t1 = tm.begin();
    TID t2 = tm.begin();

    Iterable<Long> versions = kvs.getVersions(t1, key);
    tm.beforePut(t1, keys);

    /*
     * transaction 2 executes a read AFTER we have admitted the write, but
     * BEFORE the write is applied, so it does not see the written version. It
     * has to be aborted, because if we let it read the initial version of x,
     * the schedule is no longer serializable.
     */
    try {
      tm.afterGet(t2, singleton(key, versions));
      Assert.fail("read should not be permitted");
    } catch (TransactionAbortedException e) {
      // expected
    }
    kvs.putVersion(key, 1);
    tm.afterPut(t1, keys);
    tm.commit(t1);

    // at this point we expect only version 2 of x to be present
    versions = kvs.getVersions(key);
    Iterator<Long> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t1.getTS()), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  /**
   * tests that a blocked transaction can be committed after TM restart
   */
  @Ignore
  @Test
  public void testRestart() throws IOException, InterruptedException,
      ExecutionException {
    // state in the data store
    StringKey key = new StringKey("x");
    long version = tsm.acquire();
    kvs.putVersion(key, version);

    final TID t1 = tm.begin();
    final TID t2 = tm.begin();

    kvs.getVersions(t1, key);
    kvs.putVersion(t1, key);

    kvs.getVersions(t2, key);
    kvs.putVersion(t2, key);

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
    tm.stop();

    try {
      f.get();
      Assert.fail("expected exception");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
    }

    tm = new MVTOTransactionManager<StringKey, StringKeyLogRecord>(kvs, log,
        tsm, null);// TODO fix
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
    Assert.assertEquals(Long.valueOf(t2.getTS()), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(t1.getTS()), it.next());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(Long.valueOf(0), it.next());
    Assert.assertFalse(it.hasNext());
  }

  private static Iterable<StringKeyVersions> singleton(StringKey key,
      Iterable<Long> versions) {
    return Collections.singletonList(new StringKeyVersions(key, versions));
  }
}
