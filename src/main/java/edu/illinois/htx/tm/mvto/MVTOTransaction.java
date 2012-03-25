package edu.illinois.htx.tm.mvto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyVersion;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long id;
  private State state;
  private final List<KeyVersion<K>> reads;
  private final Map<K, Boolean> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final List<MVTOTransaction<K>> readBy;

  public MVTOTransaction(long id) {
    this.id = id;
    this.state = State.ACTIVE;
    this.reads = new ArrayList<KeyVersion<K>>();
    this.writes = new HashMap<K, Boolean>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new ArrayList<MVTOTransaction<K>>(0);
  }

  long getID() {
    return this.id;
  }

  void setState(State state) {
    this.state = state;
  }

  State getState() {
    return state;
  }

  void addReadFrom(MVTOTransaction<K> transaction) {
    assert transaction.getState() != State.COMMITTED;
    readFrom.add(transaction);
  }

  void removeReadFrom(MVTOTransaction<K> transaction) {
    readFrom.remove(transaction);
  }

  Collection<MVTOTransaction<K>> getReadFrom() {
    return readFrom;
  }

  void addReadBy(MVTOTransaction<K> transaction) {
    assert this.state != State.COMMITTED;
    readBy.add(transaction);
  }

  List<MVTOTransaction<K>> getReadBy() {
    return readBy;
  }

  void addRead(KeyVersion<K> kv) {
    reads.add(kv);
  }

  public Iterable<KeyVersion<K>> getReads() {
    return reads;
  }

  void addDelete(K key) {
    writes.put(key, true);
  }

  boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
  }

  Iterable<K> getDeletes() {
    return new Iterable<K>() {
      @Override
      public Iterator<K> iterator() {
        return new FilteredKeyIterator<K, Boolean>(writes, true);
      }
    };
  }

  void addWrite(K key) {
    writes.put(key, false);
  }

  Iterable<K> getWrites() {
    return new Iterable<K>() {
      @Override
      public Iterator<K> iterator() {
        return new FilteredKeyIterator<K, Boolean>(writes, false);
      }
    };
  }

  /**
   * writes and deletes
   * 
   * @return
   */
  Iterable<K> getMutations() {
    return writes.keySet();
  }

  @Override
  public int compareTo(MVTOTransaction<K> ta) {
    return Long.valueOf(id).compareTo(ta.id);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (!(obj instanceof MVTOTransaction))
      return false;
    return id == ((MVTOTransaction<K>) obj).id;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(id).hashCode();
  }

  static class FilteredKeyIterator<T, V> implements Iterator<T> {

    private final Iterator<Entry<T, V>> it;
    private final V value;
    private T current = null;

    FilteredKeyIterator(Map<T, V> map, V value) {
      this.it = map.entrySet().iterator();
      this.value = value;
    }

    @Override
    public boolean hasNext() {
      // hasNext has been called already
      if (current != null)
        return true;
      // find next and remember
      while (it.hasNext()) {
        Entry<T, V> e = it.next();
        if (value.equals(e.getValue())) {
          current = e.getKey();
          return true;
        }
      }
      // no next found
      return false;
    }

    @Override
    public T next() {
      // hasNext has not been called
      if (current == null) {
        // loop eventually fails or returns
        while (true) {
          Entry<T, V> e = it.next();
          if (value.equals(e.getValue()))
            return e.getKey();
        }
      }
      // hasNext has been called > reset
      T tmp = current;
      current = null;
      return tmp;
    }

    @Override
    public void remove() {
      it.remove();
    }
  }
}