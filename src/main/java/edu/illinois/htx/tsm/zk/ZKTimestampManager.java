package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_LRT;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMPS;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_LRT;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_TIMESTAMPS;
import static edu.illinois.htx.tsm.zk.Util.createWithParents;
import static edu.illinois.htx.tsm.zk.Util.getId;
import static edu.illinois.htx.tsm.zk.Util.join;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.illinois.htx.tsm.NoSuchTimestampException;

public class ZKTimestampManager extends ZooKeeperListener implements
    ReclaimableTimestampManager {

  protected final List<TimestampReclamationListener> listeners;
  protected final String timestampsNode;
  protected final String timestampsDir;
  // last reclaimed timestamp node
  protected final String lrtNode;

  public ZKTimestampManager(ZooKeeperWatcher zkw) {
    super(zkw);
    this.listeners = new CopyOnWriteArrayList<TimestampReclamationListener>();
    Configuration conf = zkw.getConfiguration();
    String htx = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String timestamps = conf.get(ZOOKEEPER_ZNODE_TIMESTAMPS,
        DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMPS);
    String lrt = conf.get(ZOOKEEPER_ZNODE_LRT, DEFAULT_ZOOKEEPER_ZNODE_LRT);
    String htxNode = join(zkw.baseZNode, htx);
    this.timestampsNode = join(htxNode, timestamps);
    this.timestampsDir = Util.toDir(timestampsNode);
    this.lrtNode = join(htxNode, lrt);
  }

  public void start() {
    watcher.registerListener(this);
  }

  @Override
  public long create() throws IOException {
    try {
      String tsNode = createWithParents(watcher, timestampsDir, new byte[0],
          EPHEMERAL_SEQUENTIAL);
      return getId(tsNode);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
  }

  @Override
  public boolean delete(long ts) throws NoSuchTimestampException, IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      ZKUtil.deleteNode(watcher, tsNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getLastReclaimedTimestamp() throws IOException {
    try {
      byte[] data = ZKUtil.getData(watcher, lrtNode);
      return data == null ? 0 : Bytes.toInt(data);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean addTimestampListener(final long ts,
      final TimestampListener listener) throws IOException {
    String tsNode = Util.join(timestampsNode, ts);
    return addTimestampListener(ts, tsNode, listener);
  }

  protected boolean addTimestampListener(final long ts, final String node,
      final TimestampListener listener) throws IOException {
    return addTimestampListener(node, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
        case NodeDeleted:
          listener.deleted(ts);
        default:
          break;
        }
      }
    });
  }

  protected boolean addTimestampListener(final String node,
      final Watcher watcher) throws IOException {
    try {
      return Util.setWatch(this.watcher, node, watcher);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addTimestampReclamationListener(
      TimestampReclamationListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean hasReferences(long ts) throws NoSuchTimestampException,
      IOException {
    return false;
  }

  @Override
  public long getLastCreatedTimestamp() throws IOException {
    Stat stat = new Stat();
    try {
      return ZKUtil.getDataNoWatch(watcher, timestampsNode, stat) == null ? 0
          : stat.getCversion();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Long> getTimestamps() throws IOException {
    List<String> children;
    try {
      children = ZKUtil.listChildrenNoWatch(watcher, timestampsNode);
    } catch (KeeperException e) {
      throw new IOException();
    }
    Collections.sort(children);
    return Iterables.transform(children, new Function<String, Long>() {
      @Override
      public Long apply(String path) {
        return Long.parseLong(ZKUtil.getNodeName(path));
      }
    });
  }

  @Override
  public void setLastReclaimedTimestamp(long ts) throws IOException {
    byte[] data = Bytes.toBytes(ts);
    try {
      try {
        ZKUtil.setData(watcher, lrtNode, data);
      } catch (NoNodeException e) {
        try {
          Util.createWithParents(watcher, lrtNode, data, CreateMode.PERSISTENT);
        } catch (NodeExistsException e1) {
          setLastReclaimedTimestamp(ts);
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e1) {
      Thread.interrupted();
      throw new IOException(e1);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (!lrtNode.equals(path))
      return;
    long lrt;
    try {
      lrt = getLastReclaimedTimestamp();
    } catch (IOException e) {
      System.err.println("Couldn't get latest reclaimed timestamp node");
      e.printStackTrace();
      return;
    }
    for (TimestampReclamationListener listener : listeners)
      try {
        listener.reclaimed(lrt);
      } catch (Throwable t) {
        System.out.println("Listener failed: " + listener);
        t.printStackTrace();
      }
  }

  // TODO handle overflow
  @Override
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

}
