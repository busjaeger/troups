package edu.illinois.htx.test;

import edu.illinois.htx.tm.KeyVersions;

public class StringKeyVersions implements KeyVersions<StringKey> {

  private final StringKey key;
  private final Iterable<Long> versions;

  public StringKeyVersions(String s, Iterable<Long> versions) {
    this(new StringKey(s), versions);
  }

  public StringKeyVersions(StringKey key, Iterable<Long> versions) {
    this.key = key;
    this.versions = versions;
  }

  @Override
  public StringKey getKey() {
    return key;
  }

  @Override
  public Iterable<Long> getVersions() {
    return versions;
  }

  @Override
  public String toString() {
    return key.toString() + "-" + versions;
  }
}
