package edu.illinois.troups.tm.region;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import edu.illinois.troups.tm.KeyVersions;

public class HRegionTransactionManagerTest {

  List<KeyValue> kvs;

  @Before
  public void before() {
    byte[] row = toBytes("r1");
    byte[][] families = new byte[][] { toBytes("cf1"), toBytes("cf2") };
    byte[][] qualifiers = new byte[][] { toBytes("q1"), toBytes("q2") };
    long[] timestamps = new long[] { 1, 2 };
    byte[][] values = new byte[][] { toBytes("v1"), toBytes("v2") };

    int i = 0;
    kvs = new ArrayList<KeyValue>();
    for (byte[] family : families)
      for (byte[] qualifier : qualifiers)
        for (long timestamp : timestamps)
          kvs.add(new KeyValue(row, family, qualifier, timestamp,
              values[(i += 3) % 2]));

    Collections.sort(kvs, KeyValue.COMPARATOR);
  }

  @Test
  public void testKeyIterator() {
    Iterable<KeyVersions<HKey>> versions = HRegionTransactionManager
        .transform(kvs);
    int i = 0;
    for (KeyVersions<HKey> kvs : versions) {
      kvs.getKey();
      i++;
    }
    Assert.assertEquals(4, i);
  }

  @Test
  public void testVersionIterator() {
    Iterable<KeyVersions<HKey>> versions = HRegionTransactionManager
        .transform(kvs);
    for (KeyVersions<HKey> kv : versions) {
      for (Iterator<Long> it = kv.getVersions().iterator(); it.hasNext();) {
        it.next();
        while (it.hasNext()) {
          it.next();
          it.remove();
        }
      }
    }
    for (KeyVersions<HKey> vs : versions)
      for (Long version : vs.getVersions())
        Assert.assertEquals(Long.valueOf(2L), version);
    Assert.assertEquals(4, kvs.size());
  }

  @Test
  public void testKeys() {
    byte[] row = Bytes.toBytes("10");
    byte[] fam = Bytes.toBytes("balance");
    byte[] col = Bytes
        .toBytes("main");
    long ts1 = 11l;
    long ts2 = 10l;
    
    kvs.clear();
    KeyValue first =new KeyValue(row, fam, col, ts1, KeyValue.Type.Put);
    KeyValue second = new KeyValue(row, fam, col, ts2, KeyValue.Type.Put);
    kvs.add(first);
    kvs.add(second);
    Collections.sort(kvs, KeyValue.COMPARATOR);
    Iterable<KeyVersions<HKey>> versions = HRegionTransactionManager
        .transform(kvs);

    HKey expected = new HKey(row, fam, col);
    
    Iterator<KeyVersions<HKey>> it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(expected, it.next().getKey());
    Assert.assertFalse(it.hasNext());

    it = versions.iterator();
    Assert.assertTrue(it.hasNext());
    KeyVersions<HKey> kv = it.next();
    Assert.assertEquals(expected, kv.getKey());
    Iterator<Long> vs = kv.getVersions().iterator();
    Assert.assertTrue(vs.hasNext());
    Assert.assertEquals((Long)ts1, vs.next());
    Assert.assertTrue(vs.hasNext());
    Assert.assertEquals((Long)ts2, vs.next());
    Assert.assertFalse(vs.hasNext());
  }

  @Test
  public void testBoth() {
    testKeyIterator();
    testVersionIterator();
  }
}
