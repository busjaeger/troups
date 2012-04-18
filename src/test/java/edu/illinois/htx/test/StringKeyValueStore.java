package edu.illinois.htx.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.htx.tm.KeyValueStore;
import edu.illinois.htx.tm.KeyValueStoreObserver;
import edu.illinois.htx.tm.TransactionAbortedException;

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

  private final List<KeyValueStoreObserver<StringKey>> observers;
  private final NavigableMap<StringKey, NavigableMap<Long, Object>> values;

  public StringKeyValueStore() {
    this.values = new TreeMap<StringKey, NavigableMap<Long, Object>>();
    this.observers = new ArrayList<KeyValueStoreObserver<StringKey>>();
  }

  public void writeVersionObserved(long tid, StringKey key)
      throws TransactionAbortedException, IOException {
    Iterable<StringKey> keys = Arrays.asList(key);
    for (KeyValueStoreObserver<StringKey> observer : observers)
      observer.beforeWrite(tid, false, keys);
    writeVersion(key, tid);
    for (KeyValueStoreObserver<StringKey> observer : observers)
      observer.afterWrite(tid, false, keys);
  }

  public Iterable<Long> readVersionsObserved(long tid, final StringKey key)
      throws TransactionAbortedException, IOException {
    for (KeyValueStoreObserver<StringKey> observer : observers) {
      Iterable<StringKey> keys = Arrays.asList(key);
      observer.beforeRead(tid, keys);
    }
    Iterable<Long> versions = readVersions(key, tid);
    for (KeyValueStoreObserver<StringKey> observer : observers) {
      Iterable<StringKeyVersions> kvs = asList(new StringKeyVersions(key,
          versions));
      observer.afterRead(tid, kvs);
    }
    return versions;
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

  public Iterable<Long> readVersions(StringKey key) {
    return readVersions(key, Integer.MAX_VALUE);
  }

  public Iterable<Long> readVersions(StringKey key, long max) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      return emptyList();
    return new ArrayList<Long>(versions.tailMap(max).keySet());
  }

  public void writeVersion(StringKey key, long version) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      values.put(key, versions = new TreeMap<Long, Object>(VersionComparator));
    versions.put(version, new Object());
  }

  @Override
  public void addObserver(KeyValueStoreObserver<StringKey> observer) {
  }
}
