package edu.illinois.htx.regionserver;

import org.apache.hadoop.hbase.KeyValue;

import com.google.common.base.Function;

import edu.illinois.htx.tm.KeyVersion;

public class HKeyVersion implements KeyVersion<HKey> {

  static final Function<KeyValue, HKeyVersion> KEYVALUE_TO_KEYVERSION = new Function<KeyValue, HKeyVersion>() {
    @Override
    public HKeyVersion apply(KeyValue kv) {
      return new HKeyVersion(kv);
    }
  };

  private final HKey key;
  private final long version;

  HKeyVersion(KeyValue kv) {
    this(new HKey(kv), kv.getTimestamp());
  }

  public HKeyVersion(byte[] row, byte[] family, byte[] qualifier, long version) {
    this(new HKey(row, family, qualifier), version);
  }

  public HKeyVersion(HKey key, long version) {
    this.key = key;
    this.version = version;
  }

  @Override
  public HKey getKey() {
    return key;
  }

  @Override
  public long getVersion() {
    return version;
  }

}
