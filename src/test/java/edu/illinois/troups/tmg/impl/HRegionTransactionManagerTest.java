package edu.illinois.troups.tmg.impl;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;

import edu.illinois.troups.tm.KeyVersions;
import edu.illinois.troups.tmg.impl.HKey;
import edu.illinois.troups.tmg.impl.HRegionTransactionManager;

public class HRegionTransactionManagerTest {

  @Test
  public void testTransform() {
    byte[] row = toBytes("r1");
    byte[][] families = new byte[][] { toBytes("cf1"), toBytes("cf2") };
    byte[][] qualifiers = new byte[][] { toBytes("q1"), toBytes("q2") };
    long[] timestamps = new long[] { 1, 2 };
    byte[][] values = new byte[][] { toBytes("v1"), toBytes("v2") };

    int i = 0;
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    for (byte[] family : families)
      for (byte[] qualifier : qualifiers)
        for (long timestamp : timestamps)
          kvs.add(new KeyValue(row, family, qualifier, timestamp,
              values[(i += 3) % 2]));

    Collections.sort(kvs, KeyValue.COMPARATOR);

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
}
