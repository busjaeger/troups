package edu.illinois.htx.test;

import edu.illinois.htx.tm.KeyVersion;

public class StringKeyVersion implements KeyVersion<StringKey> {

  private final StringKey key;
  private final long version;

  public StringKeyVersion(String s, long version) {
    this(new StringKey(s), version);
  }

  public StringKeyVersion(StringKey key, long version) {
    this.key = key;
    this.version = version;
  }

  @Override
  public StringKey getKey() {
    return key;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StringKeyVersion))
      return false;
    StringKeyVersion v = (StringKeyVersion) obj;
    return v.key.equals(key) && v.version == version;
  }

  @Override
  public int hashCode() {
    return key.hashCode() * Long.valueOf(version).hashCode();
  }

  @Override
  public String toString() {
    return key.toString() + ":" + version;
  }
}
