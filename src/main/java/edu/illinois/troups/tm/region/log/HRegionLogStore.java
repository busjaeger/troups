package edu.illinois.troups.tm.region.log;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

import edu.illinois.troups.tm.region.HKey;

/**
 * This idea doesn't work: trying to call put on the region while it is already
 * executing a put on that row causes an issue with the row locks
 */
public class HRegionLogStore implements GroupLogStore {

  private final HRegion region;
  private final byte[] logFamily;

  public HRegionLogStore(HRegion region, byte[] logFamily) {
    this.region = region;
    this.logFamily = logFamily;
  }

  @Override
  public NavigableMap<Long, byte[]> open(HKey groupKey) throws IOException {
    Get get = new Get(groupKey.getRow());
    get.addFamily(logFamily);
    Result result = region.get(get, null);
    NavigableMap<Long, byte[]> records = new TreeMap<Long, byte[]>();
    if (result.getMap() != null) {
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columns = result
          .getMap().get(logFamily);
      if (columns != null) {
        records = columns.get(null);
      }
    }
    return records;
  }

  @Override
  public void append(HKey groupKey, long sid, byte[] value) throws IOException {
    Put put = new Put(groupKey.getRow(), sid);
    put.add(logFamily, null, value);
    region.put(put);
  }

  @Override
  public void truncate(HKey groupKey, long sid) throws IOException {
    Delete delete = new Delete(groupKey.getRow());
    delete.deleteColumns(logFamily, null, sid);
    region.delete(delete, null, true);
  }

}
