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
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.util.ZKUtil;

public class DistributedTransaction extends AbstractTransaction implements
    Transaction, Watcher {

  private final Set<RowGroup> groups = new HashSet<RowGroup>();
  private final Map<Integer, Boolean> votes = new TreeMap<Integer, Boolean>();
  private boolean committing = false;

  DistributedTransaction(long id, ZooKeeperWatcher zkw, String node,
      String eNode) {
    super(id, zkw, node, eNode);
  }

  @Override
  public void enlist(HTable table, byte[] row) throws IOException {
    HTableDescriptor descr = table.getTableDescriptor();
    byte[] tableName = descr.getName();
    // TODO remove hard-coded split policy
    byte[] rootRow = Arrays.copyOf(row, Math.min(4, row.length));
    RowGroup group = new RowGroup(tableName, rootRow);

    // this is a new row group -> enlist RTM in transaction
    if (groups.add(group)) {
      int partId = getRTM(table, rootRow).enlist(id);
      String partNode = ZKUtil.joinZNode(tranNode, String.valueOf(partId));
      if (!ZKUtil.setWatch(zkw, partNode, this)) {
        rollback();
        throw new TransactionAbortedException("Participant failed");
      }
    }
  }

  @Override
  public synchronized void rollback() {
    if (done)
      return;
    if (committing)
      throw new IllegalStateException("Cannot rollback after commit issued");
    done();
  }

  @Override
  public synchronized void commit() throws TransactionAbortedException {
    if (done)
      return;
    if (committing)
      throw new IllegalStateException("Already committing");
    initPrepare();
    while (!done) {
      try {
        wait();
      } catch (InterruptedException e) {
        // TODO
        Thread.interrupted();
      }
    }
  }

  // 1. Phase: prepare
  private synchronized void initPrepare() throws TransactionAbortedException {
    try {
      ZKUtil.setData(zkw, clientNode, Bytes.toBytes("prepared"));
    } catch (IOException e) {
      rollback();
      throw new TransactionAbortedException(e);
    }
  }

  // 2. Phase: commit
  private synchronized void initCommitting() {
    Map<Integer, Boolean> doneVotes = new TreeMap<Integer, Boolean>(votes);
    for (Entry<Integer, Boolean> entry : doneVotes.entrySet())
      entry.setValue(false);
    byte[] state = WritableUtils.toByteArray(new TransactionState(false,
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
        int partId = Integer.parseInt(ZKUtil.getNodeName(path));
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
          TransactionState state = new TransactionState();
          try {
            state.readFields(ZKUtil.getData(zkw, tranNode));
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
}
