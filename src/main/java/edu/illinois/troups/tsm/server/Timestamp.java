package edu.illinois.troups.tsm.server;

public class Timestamp {

  protected final long creationTime;
  protected boolean released;

  public Timestamp() {
    this(false, System.currentTimeMillis());
  }

  public Timestamp(boolean released, long creationTime) {
    this.released = released;
    this.creationTime = creationTime;
  }

  public synchronized boolean isReleased() {
    return released;
  }

  public synchronized boolean release() {
    if (released)
      return false;
    return released = true;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public boolean isPersisted() {
    return false;
  }
}