package edu.illinois.htx.tm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Transaction {

  public static enum State {
    ACTIVE, BLOCKED, ABORTED, COMMITTED;
  }

  private final long tts;
  private State state;
  private final Set<Key> written;
  private final Set<Key> deleted;
  private final Map<Key, Long> read;
  private final Set<Transaction> readFrom;
  private final List<Transaction> readBy;

  public Transaction(long tts) {
    this.tts = tts;
    this.state = State.ACTIVE;
    this.written = new HashSet<Key>();
    this.deleted = new HashSet<Key>();
    this.read = new HashMap<Key, Long>();
    this.readFrom = new HashSet<Transaction>(0);
    this.readBy = new ArrayList<Transaction>(0);
  }

  long getTransactionTimestamp() {
    return this.tts;
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

  public Collection<Transaction> getReadFrom() {
    return readFrom;
  }

  void addReadBy(Transaction transaction) {
    assert this.state != State.COMMITTED;
    readBy.add(transaction);
  }

  public List<Transaction> getReadBy() {
    return readBy;
  }

  void addDelete(Key key) {
    deleted.add(key);
  }

  public Iterable<Key> getDeleted() {
    return deleted;
  }

  boolean hasDeleted(Key key) {
    return deleted.contains(key);
  }

  void addWritten(Key key) {
    written.add(key);
  }

  boolean hasWritten(Key key) {
    return written.contains(key);
  }

  public Iterable<Key> getWritten() {
    return written;
  }

  void addRead(Key key, long timestamp) {
    read.put(key, timestamp);
  }

  Long getTimestampRead(Key key) {
    return read.get(key);
  }

}