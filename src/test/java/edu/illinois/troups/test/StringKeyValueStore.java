package edu.illinois.troups.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import edu.illinois.troups.tm.KeyValueStore;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tm.TransactionOperationObserver;

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

  private final List<TransactionOperationObserver<StringKey>> observers;
  private final NavigableMap<StringKey, NavigableMap<Long, Object>> values;

  public StringKeyValueStore() {
    this.values = new TreeMap<StringKey, NavigableMap<Long, Object>>();
    this.observers = new ArrayList<TransactionOperationObserver<StringKey>>();
  }

  public void putVersion(TID tid, StringKey key)
      throws TransactionAbortedException, IOException {
    Iterable<StringKey> keys = Arrays.asList(key);
    for (TransactionOperationObserver<StringKey> observer : observers)
      observer.beforePut(tid, keys);
    putVersion(key, tid.getTS());
    for (TransactionOperationObserver<StringKey> observer : observers)
      observer.afterPut(tid, keys);
  }

  public Iterable<Long> getVersions(TID tid, final StringKey key)
      throws TransactionAbortedException, IOException {
    for (TransactionOperationObserver<StringKey> observer : observers) {
      Iterable<StringKey> keys = Arrays.asList(key);
      observer.beforeGet(tid, keys);
    }
    Iterable<Long> versions = getVersions(key, tid.getTS());
    for (TransactionOperationObserver<StringKey> observer : observers) {
      List<StringKeyVersions> kvs = asList(new StringKeyVersions(key,
          versions));
      observer.afterGet(tid, kvs.size(), kvs);
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

  public Iterable<Long> getVersions(StringKey key) {
    return getVersions(key, Integer.MAX_VALUE);
  }

  public Iterable<Long> getVersions(StringKey key, long max) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      return emptyList();
    return new ArrayList<Long>(versions.tailMap(max).keySet());
  }

  public void putVersion(StringKey key, long version) {
    NavigableMap<Long, Object> versions = values.get(key);
    if (versions == null)
      values.put(key, versions = new TreeMap<Long, Object>(VersionComparator));
    versions.put(version, new Object());
  }

  public void addTransactionOperationObserver(
      TransactionOperationObserver<StringKey> observer) {
    observers.add(observer);
  }

}
