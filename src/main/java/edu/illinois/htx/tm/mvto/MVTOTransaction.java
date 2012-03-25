package edu.illinois.htx.tm.mvto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;

import edu.illinois.htx.tm.Key;
import edu.illinois.htx.tm.KeyVersion;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long id;
  private State state;
  private final List<KeyVersion<K>> reads;
  private final List<K> writes;
  private final Set<K> deletes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final List<MVTOTransaction<K>> readBy;

  public MVTOTransaction(long id) {
    this.id = id;
    this.state = State.ACTIVE;
    this.reads = new ArrayList<KeyVersion<K>>();
    this.writes = new ArrayList<K>();
    this.deletes = new HashSet<K>();
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

  public List<KeyVersion<K>> getReads() {
    return reads;
  }

  void addWrite(K key) {
    writes.add(key);
  }

  List<K> getWrites() {
    return writes;
  }

  void addDelete(K key) {
    deletes.add(key);
  }

  boolean hasDeleted(K key) {
    return deletes.contains(key);
  }

  Iterable<K> getDeletes() {
    return deletes;
  }

  /**
   * writes and deletes
   * 
   * @return
   */
  Iterable<K> getMutations() {
    return Iterables.concat(writes, deletes);
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

}