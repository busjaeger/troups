package edu.illinois.htx.test;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.htx.tm.KeyValueStore;

/**
 * not thread safe. For testing purposes only
 */
public class StringKeyValueStore implements KeyValueStore<StringKey> {

  private static Comparator<Long> VersionComparator = new Comparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      return o2.compareTo(o1);
    }
  };

  private final NavigableMap<StringKey, NavigableMap<Long, Object>> values;

  public StringKeyValueStore() {
    this.values = new TreeMap<StringKey, NavigableMap<Long, Object>>();
  }

  public void writeVersion(StringKey key, long version) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      values.put(key, versions = new TreeMap<Long, Object>(VersionComparator));
    versions.put(version, new Object());
  }

  public Iterable<Long> readVersions(final StringKey key) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      return emptyList();
    return new ArrayList<Long>(versions.keySet());
  }

  @Override
  public void deleteVersion(StringKey key, long version) throws IOException {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      return;
    versions.remove(version);
    if (versions.isEmpty())
      values.remove(key);
  }

  @Override
  public void deleteVersions(StringKey key, long version) throws IOException {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      return;
    versions.headMap(version, true).clear();
    if (versions.isEmpty())
      values.remove(key);
  }

}
