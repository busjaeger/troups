package edu.illinois.htx.client.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.regionserver.RTM;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.TimestampState;
import edu.illinois.htx.tsm.XATimestampManager;
import edu.illinois.htx.tsm.XATimestampManager.ParticipantListener;

public class DistributedTransaction implements Transaction, Watcher {

  private static enum State {
    CREATED, ACTIVE, COMMITTING, DONE
  }

  private long id;
  private final XATimestampManager tsm;
  private final Set<RowGroup> groups = new HashSet<RowGroup>();
  private final Map<Long, Boolean> votes = new TreeMap<Long, Boolean>();
  private State state;

  DistributedTransaction(XATimestampManager tsm) {
    this.tsm = tsm;
    this.state = State.CREATED;
  }

  void begin() throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ACTIVE:
    case COMMITTING:
    case DONE:
      throw newISA("begin");
    }
    this.id = tsm.next();
  }

  @Override
  public long enlist(HTable table, byte[] row) throws IOException {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTING:
    case CREATED:
    case DONE:
      throw newISA("enlist");
    }

    // TODO remove hard-coded split policy
    byte[] rootRow = Arrays.copyOf(row, Math.min(4, row.length));
    RowGroup group = new RowGroup(table, rootRow);

    // this is a new row group -> enlist RTM in transaction
    if (groups.add(group)) {
      long pid = getRTM(group).join(id);
      if (!tsm.addListener(id, pid, new RTMListener(pid))) {
        rollback();
        throw new TransactionAbortedException();
      }
    }
    return this.id;
  }

  protected RTM getRTM(RowGroup group) {
    return group.getTable().coprocessorProxy(RTM.class, group.getRootRow());
  }

  @Override
  public synchronized void rollback() {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTING:
    case CREATED:
      throw newISA("rollback");
    case DONE:
      return;
    }

    // TODO use separate threads
    try {
      for (RowGroup group : groups)
        getRTM(group).abort(id);
    } catch (IOException e) {
      e.printStackTrace();
      // ignore
    }

    try {
      tsm.done(id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void commit() throws TransactionAbortedException {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTING:
    case CREATED:
      throw newISA("rollback");
    case DONE:
      return;
    }

    // 1. Phase: ask all participants to prepare
    // TODO use separate threads
    try {
      for (RowGroup group : groups)
        getRTM(group).prepare(id);
    } catch (IOException e) {
      rollback();
      throw new TransactionAbortedException(e);
    }

    while (state != State.DONE) {
      try {
        wait();
      } catch (InterruptedException e) {
        // TODO
        Thread.interrupted();
      }
    }
  }

  synchronized void setPrepared(long partId) {
    votes.put(partId, true);
    for (Boolean vote : votes.values())
      if (!vote)
        return;

    // 2. Phase: all participants have vowed to commit > write record
    Map<Long, Boolean> doneVotes = new TreeMap<Long, Boolean>(votes);
    for (Entry<Long, Boolean> entry : doneVotes.entrySet())
      entry.setValue(false);
    byte[] state = WritableUtils.toByteArray(new TimestampState(false,
        doneVotes));
    
    
    try {
      ZKUtil.setWatch(zkw, tranNode, this);
      ZKUtil.setData(zkw, tranNode, state);
    } catch (IOException e) {
      rollback();
      return;
    }
    committing = false;
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    if (done)
      return;
    // preparing phase
    if (!committing) {
      switch (event.getType()) {
      case NodeDeleted:
        rollback();
        break;
      case NodeDataChanged:
        String path = event.getPath();
        long partId = Long.parseLong(ZKUtil.getNodeName(path));
        votes.put(partId, true);
        for (Boolean vote : votes.values())
          if (!vote)
            return;
        initCommitting();
        break;
      default:
        break;
      }
    } else {
      switch (event.getType()) {
      case NodeDataChanged:
        String path = event.getPath();
        if (path.equals(tranNode)) {
          TimestampState state;
          try {
            state = new TimestampState(ZKUtil.getData(zkw, tranNode));
          } catch (IOException e) {
            // TODO probably OK to ignore
            return;
          }
          for (Boolean vote : state.getVotes().values())
            if (!vote)
              return;
          done();
          notify();
        }
      default:
        break;
      }
    }
  }

  private IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  private class RTMListener implements ParticipantListener {

    private final long pid;

    RTMListener(long pid) {
      this.pid = pid;
    }

    @Override
    public void prepared() {
      setPrepared(pid);
    }

    @Override
    public void aborted() {
      rollback();
    }

    @Override
    public void committed() {
    }

  }
}
