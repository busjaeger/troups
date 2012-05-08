package edu.illinois.troups.tsm.server;

import java.util.HashSet;
import java.util.Set;

public class SharedTimestamp extends Timestamp {

  private final Set<Long> references;
  private long referenceCounter;
  private boolean persisted;

  SharedTimestamp() {
    super();
    this.references = new HashSet<Long>();
    this.referenceCounter = 0;
  }

  SharedTimestamp(Iterable<Long> rids) {
    this();
    this.persisted = true;
    for (Long rid : rids)
      references.add(rid);
  }

  public Set<Long> getRIDs() {
    return references;
  }

  public boolean isPersisted() {
    return persisted;
  }

  public void setPersisted(boolean persisted) {
    this.persisted = persisted;
  }

  public long nextReferenceID() {
    return referenceCounter++;
  }

}
