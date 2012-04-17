package edu.illinois.htx.client.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HTable;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.regionserver.RTM;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.TimestampState;
import edu.illinois.htx.tsm.Version;
import edu.illinois.htx.tsm.XATimestampManager;
import edu.illinois.htx.tsm.XATimestampManager.ParticipantListener;

public class XGTransaction implements Transaction, ParticipantListener {

  private static enum State {
    CREATED, ACTIVE, PREPARED, COMMITTING, COMMITTED, ABORTED
  }

  private long id;
  private final XATimestampManager tsm;
  private final Set<RowGroup> groups = new HashSet<RowGroup>();
  private final Map<Long, Boolean> votes = new TreeMap<Long, Boolean>();
  private State state;

  XGTransaction(XATimestampManager tsm) {
    this.tsm = tsm;
    this.state = State.CREATED;
  }

  void begin() throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ACTIVE:
    case PREPARED:
    case COMMITTING:
    case COMMITTED:
    case ABORTED:
      throw newISA("begin");
    }
    id = tsm.next();
    state = State.ACTIVE;
  }

  @Override
  public long enlist(HTable table, byte[] row) throws IOException {
    switch (state) {
    case ACTIVE:
      break;
    case CREATED:
    case PREPARED:
    case COMMITTING:
    case COMMITTED:
    case ABORTED:
      throw newISA("enlist");
    }

    // TODO remove hard-coded split policy
    byte[] rootRow = Arrays.copyOf(row, Math.min(4, row.length));
    RowGroup group = new RowGroup(table, rootRow);

    // this is a new row group -> enlist RTM in transaction
    if (!groups.contains(group)) {
      long pid = getRTM(group).join(id);
      groups.add(group);
      if (!tsm.addParticipantListener(id, pid, this)) {
        rollback();
        throw new TransactionAbortedException();
      }
    }
    return id;
  }

  @Override
  public synchronized void rollback() {
    switch (state) {
    case ACTIVE:
    case PREPARED:
      break;
    case CREATED:
    case COMMITTING:
    case COMMITTED:
      throw newISA("rollback");
    case ABORTED:
      return;
    }

    // TODO use separate threads
    try {
      for (RowGroup group : groups)
        getRTM(group).abort(id);
    } catch (IOException e) {
      // some may have already disconnected
    }

    try {
      tsm.done(id);
    } catch (IOException e) {
      // assume if we can't connect it's already marked done
      e.printStackTrace();
    }
    state = State.ABORTED;
    notify();
  }

  @Override
  public synchronized void commit() throws TransactionAbortedException {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTED:
      return;
    case CREATED:
    case PREPARED:
    case COMMITTING:
    case ABORTED:
      throw newISA("commit");
    }

    state = State.PREPARED;
    // 1. Phase: ask all participants to prepare
    // TODO use separate threads
    try {
      for (RowGroup group : groups)
        getRTM(group).prepare(id);
    } catch (IOException e) {
      rollback();
      throw new TransactionAbortedException(e);
    }

    while (!(state == State.COMMITTED || state == State.ABORTED)) {
      try {
        wait();
      } catch (InterruptedException e) {
        // TODO
        Thread.interrupted();
      }
    }
    if (state == State.ABORTED)
      throw new TransactionAbortedException();
  }

  @Override
  public void disconnected(long pid) {
    switch (state) {
    case CREATED:
    case ACTIVE:
      System.err.print(this + " received disconnected from " + pid);
      break;
    case ABORTED:
    case COMMITTING:
    case COMMITTED:
      return;
    case PREPARED:
      break;
    }
    // here we have a choice > current design: abort
    rollback();
  }

  @Override
  public synchronized void prepared(long pid) {
    switch (state) {
    case CREATED:
    case ACTIVE:
    case PREPARED:
      break;
    case ABORTED:
      return;
    case COMMITTING:
    case COMMITTED:
      System.err.println(this + " received prepared from " + pid);
      return;
    }

    votes.put(pid, true);
    for (Boolean vote : votes.values())
      if (!vote)
        return;

    /*
     * 2. Phase: all participants have vowed to commit: after we have
     * successfully created the commit record, the transaction must commit
     * eventually, i.e. regardless of client or server failures, the transaction
     * cannot be rolled back at this point.
     */
    try {
      tsm.setState(id, new TimestampState(votes.keySet()), new Version(0));
    } catch (IOException e) {
      rollback();
      return;
    }
    state = State.COMMITTING;
    // TODO use separate threads
    // TODO think if better to deliver commit through ZK
    for (RowGroup group : groups)
      while (true) {
        try {
          getRTM(group).commit(id);
          break;
        } catch (IOException e) {
          e.printStackTrace();
          try {
            Thread.sleep(100);
          } catch (InterruptedException e1) {
            Thread.interrupted();
          }
        }
      }
  }

  @Override
  public void aborted(long pid) {
    switch (state) {
    case PREPARED:
      break;
    case ABORTED:
      return;
    case CREATED:
    case ACTIVE:
    case COMMITTING:
    case COMMITTED:
      System.err.println(this + " received prepared from " + pid);
      return;
    }
    rollback();
  }

  @Override
  public synchronized void committed(long pid) {
    switch (state) {
    case COMMITTING:
      break;
    case COMMITTED:
      return;
    case ACTIVE:
    case ABORTED:
    case CREATED:
    case PREPARED:
      System.err.println(this + " received prepared from " + pid);
      return;
    }
    votes.put(pid, false);
    for (Boolean vote : votes.values())
      if (vote)
        return;
    try {
      tsm.done(id);
    } catch (IOException e) {
      // ignore: TSM knows all participants have completed
    }
    state = State.COMMITTED;
    notify();
  }

  protected RTM getRTM(RowGroup group) {
    return group.getTable().coprocessorProxy(RTM.class, group.getRootRow());
  }

  private IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  @Override
  public String toString() {
    return id + "(" + state + ")";
  }

}
