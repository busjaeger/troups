package edu.illinois.htx.tm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Transaction implements Comparable<Transaction> {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long id;
  private State state;
  private final Map<Key, Boolean> writes;
  private final Set<Transaction> readFrom;
  private final List<Transaction> readBy;

  public Transaction(long id) {
    this.id = id;
    this.state = State.ACTIVE;
    this.writes = new HashMap<Key, Boolean>();
    this.readFrom = new HashSet<Transaction>(0);
    this.readBy = new ArrayList<Transaction>(0);
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

  void addReadFrom(Transaction transaction) {
    assert transaction.getState() != State.COMMITTED;
    readFrom.add(transaction);
  }

  void removeReadFrom(Transaction transaction) {
    readFrom.remove(transaction);
  }

  Collection<Transaction> getReadFrom() {
    return readFrom;
  }

  void addReadBy(Transaction transaction) {
    assert this.state != State.COMMITTED;
    readBy.add(transaction);
  }

  List<Transaction> getReadBy() {
    return readBy;
  }

  void addWrite(Key key, boolean isDelete) {
    writes.put(key, isDelete);
  }

  boolean hasDeleted(Key key) {
    return Boolean.TRUE == writes.get(key);
  }

  @Override
  public int compareTo(Transaction ta) {
    return Long.valueOf(id).compareTo(ta.id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (!(obj instanceof Transaction))
      return false;
    return id == ((Transaction) obj).id;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(id).hashCode();
  }
}