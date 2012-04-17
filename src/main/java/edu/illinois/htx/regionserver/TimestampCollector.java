package edu.illinois.htx.regionserver;

import static edu.illinois.htx.HTXConstants.DEFAULT_TM_GC_INTERVAL;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_OAT;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS;
import static edu.illinois.htx.HTXConstants.TM_GC_INTERVAL;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_COLLECTORS;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_OAT;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_TRANSACTIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import edu.illinois.htx.tsm.TimestampListener;
import edu.illinois.htx.tsm.TimestampState;
import edu.illinois.htx.tsm.zk.ZKUtil;

public class TimestampCollector extends ZooKeeperListener implements Runnable {

  private final Configuration conf;
  private final ScheduledExecutorService pool;
  private final List<TimestampListener> listeners;

  private String zNode;

  // processing data
  private String transZNode;
  private String oatZNode;
  private boolean oatSet;
  private int oat;

  public TimestampCollector(Configuration conf, ScheduledExecutorService pool,
      ZooKeeperWatcher zkw) {
    super(zkw);
    this.conf = conf;
    this.pool = pool;
    this.listeners = new CopyOnWriteArrayList<TimestampListener>();
  }

  void start() throws IOException {
    watcher.registerListener(this);
    String collectors = conf.get(ZOOKEEPER_ZNODE_COLLECTORS,
        DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS);
    String collectorsNode = ZKUtil.joinZNode(getHTXZNode(), collectors);
    String collectorsDir = ZKUtil.appendSeparator(collectorsNode);
    zNode = ZKUtil.createWithParents(watcher, collectorsDir,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    tryToBecomeCollector();
    nodeDataChanged(getOATZNode());// get initial OAT
  }

  void addListener(TimestampListener listener) {
    this.listeners.add(listener);
  }

  private void tryToBecomeCollector() throws IOException {
    if (becomeCollector()) {
      long gcInterval = conf.getLong(TM_GC_INTERVAL, DEFAULT_TM_GC_INTERVAL);
      pool.scheduleAtFixedRate(this, gcInterval, gcInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  private boolean becomeCollector() throws IOException {
    List<String> children = ZKUtil
        .getChildren(watcher, ZKUtil.getParent(zNode));
    String preceeding = null;
    // loop until we are the leader or we follow someone
    while (true) {
      for (String child : children)
        if (child.compareTo(zNode) < 0)
          if (preceeding == null || preceeding.compareTo(child) < 0)
            preceeding = child;
      // this is the leader
      if (preceeding == null)
        return true;
      if (ZKUtil.setWatch(watcher, preceeding, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          switch (event.getType()) {
          case NodeDeleted:
            try {
              tryToBecomeCollector();
            } catch (IOException e) {
              e.printStackTrace();
              // TODO remove ephemeral node, retry etc.
            }
            break;
          default:
            break;
          }
        }
      }))
        break;
    }
    return false;
  }

  @Override
  public void nodeDataChanged(String path) {
    if (getOATZNode().equals(path)) {
      try {
        long oat = getOAT();
        for (TimestampListener listener : listeners)
          listener.deleted(oat);
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("Failed to retrieve OAT");
      }
    }
  }

  @Override
  public void run() {
    try {
      if (!oatSet) {
        initOAT();
        oatSet = true;
      }

      List<String> children = ZKUtil.getChildren(watcher, transZNode);
      Collections.sort(children);

      int newOat = oat;
      List<String> toDelete = new ArrayList<String>();
      for (String child : children) {
        int tid = Integer.parseInt(ZKUtil.getNodeName(child));
        if (tid < oat)
          toDelete.add(child);
        else if (tid >= oat) {
          byte[] data = ZKUtil.getData(watcher, child);
          if (data == null) {
            System.err.println("node " + child + " disappeared");
            continue;
          }
          TimestampState state = new TimestampState(data);
          if (!state.isDone()) {
            String owner = ZKUtil.joinZNode(child, "owner");
            if (ZKUtil.exists(watcher, owner) || state.hasActiveParticipants()) {
              // timestamp is still in use
              oat = tid;
              break;
            }
          }
          toDelete.add(child);
        }
      }

      if (newOat != oat) {
        ZKUtil.setData(watcher, getOATZNode(), Bytes.toBytes(newOat));
        oat = newOat;
      }

      for (String delete : toDelete)
        ZKUtil.delete(watcher, delete);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void initOAT() throws IOException {
    String oatZNode = getOATZNode();
    byte[] data = ZKUtil.getData(watcher, oatZNode);
    if (data == null) {
      data = Bytes.toBytes(oat = 0);
      ZKUtil.create(watcher, oatZNode, data, CreateMode.PERSISTENT);
    } else {
      oat = Bytes.toInt(data);
    }
  }

  private long getOAT() throws IOException {
    String oatZNode = getOATZNode();
    byte[] data = ZKUtil.getData(watcher, oatZNode);
    return data == null ? 0 : Bytes.toInt(data);
  }

  String getTransZNode() {
    if (transZNode == null) {
      String trans = conf.get(ZOOKEEPER_ZNODE_TRANSACTIONS,
          DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS);
      transZNode = ZKUtil.joinZNode(getHTXZNode(), trans);
    }
    return transZNode;
  }

  String getOATZNode() {
    if (oatZNode == null) {
      String oat = conf.get(ZOOKEEPER_ZNODE_OAT, DEFAULT_ZOOKEEPER_ZNODE_OAT);
      oatZNode = ZKUtil.joinZNode(getHTXZNode(), oat);
    }
    return oatZNode;
  }

  String getHTXZNode() {
    String htx = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    return ZKUtil.joinZNode(watcher.baseZNode, htx);
  }
}
