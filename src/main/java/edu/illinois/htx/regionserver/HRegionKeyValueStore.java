package edu.illinois.htx.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.regionserver.HRegion;

import edu.illinois.htx.tm.KeyValueStore;

public class HRegionKeyValueStore implements KeyValueStore<HKey> {

  private final HRegion region;

  public HRegionKeyValueStore(HRegion region) {
    this.region = region;
  }

  @Override
  public void deleteVersion(HKey key, long version) throws IOException {
    Delete delete = new Delete(key.getRow());
    delete.deleteColumn(key.getFamily(), key.getQualifier(), version);
    region.delete(delete, null, true);
  }

  @Override
  public void deleteVersions(HKey key, long version) throws IOException {
    Delete delete = new Delete(key.getRow());
    delete.deleteColumn(key.getFamily(), key.getQualifier(), version);
    region.delete(delete, null, true);
  }

}
