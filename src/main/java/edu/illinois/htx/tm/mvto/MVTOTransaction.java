package edu.illinois.htx.tm.mvto;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.illinois.htx.tm.Key;

class MVTOTransaction<K extends Key> implements Comparable<MVTOTransaction<K>> {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long id;
  private State state;
  private boolean finalized;
  private final Map<K, Long> reads;
  private final Map<K, Boolean> writes;
  private final Set<MVTOTransaction<K>> readFrom;
  private final Set<MVTOTransaction<K>> readBy;

  public MVTOTransaction(long id) {
    this.id = id;
    this.state = State.ACTIVE;
    this.reads = new HashMap<K, Long>();
    this.writes = new HashMap<K, Boolean>();
    this.readFrom = new HashSet<MVTOTransaction<K>>(0);
    this.readBy = new HashSet<MVTOTransaction<K>>(0);
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

  void setFinalized() {
    assert state == State.COMMITTED || state == State.ABORTED;
    this.finalized = true;
  }

  boolean isFinalized() {
    return finalized;
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

  Collection<MVTOTransaction<K>> getReadBy() {
    return readBy;
  }

  void addRead(K key, long version) {
    reads.put(key, version);
  }

  public Map<K, Long> getReads() {
    return reads;
  }

  void addWrite(K key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  Map<K, Boolean> getWrites() {
    return writes;
  }

  boolean hasDeleted(K key) {
    return Boolean.TRUE == writes.get(key);
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

  @Override
  public String toString() {
    return id + ":" + state + (finalized ? "(finalized)" : "");
  }
}