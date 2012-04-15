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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import edu.illinois.htx.util.ZKUtil;

public class TimestampCollector implements Runnable {

  private final Configuration conf;
  private final ScheduledExecutorService pool;
  private final ZooKeeperWatcher zkw;

  private String zNode;

  // processing data
  private String transZNode;
  private String oatZNode;
  private boolean oatSet;
  private int oat;

  public TimestampCollector(Configuration conf, ScheduledExecutorService pool,
      ZooKeeperWatcher zkw) {
    this.conf = conf;
    this.pool = pool;
    this.zkw = zkw;
  }

  void start() throws IOException {
    String collectors = conf.get(ZOOKEEPER_ZNODE_COLLECTORS,
        DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS);
    String collectorsNode = ZKUtil.joinZNode(getHTXZNode(), collectors);
    String collectorsDir = ZKUtil.appendSeparator(collectorsNode);
    zNode = ZKUtil.createSequentialWithParents(zkw, collectorsDir,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    tryToBecomeCollector();
  }

  private void tryToBecomeCollector() throws IOException {
    if (becomeCollector()) {
      long gcInterval = conf.getLong(TM_GC_INTERVAL, DEFAULT_TM_GC_INTERVAL);
      pool.scheduleAtFixedRate(this, gcInterval, gcInterval,
          TimeUnit.MILLISECONDS);
    }
  }

  private boolean becomeCollector() throws IOException {
    List<String> children = ZKUtil.getChildren(zkw, ZKUtil.getParent(zNode));
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
      if (ZKUtil.setWatch(zkw, preceeding, new Watcher() {
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
  public void run() {
    try {
      if (!oatSet) {
        setOAT();
        oatSet = true;
      }

      List<String> children = ZKUtil.getChildren(zkw, transZNode);
      Collections.sort(children);

      int newOat = oat;
      List<String> toDelete = new ArrayList<String>();
      for (String child : children) {
        int tid = Integer.parseInt(ZKUtil.getNodeName(child));
        if (tid < oat)
          toDelete.add(child);
        else if (tid >= oat) {
          byte[] data = ZKUtil.getData(zkw, child);
          if (data == null) {
            System.err.println("node " + child + " disappeared");
            continue;
          }
          if (data.length == 0) {
            if (!ZKUtil.getChildren(zkw, child).isEmpty()) {
              // transaction is active
              oat = tid;
              break;
            }
          }
          toDelete.add(child);
        }
      }

      if (newOat != oat) {
        ZKUtil.setData(zkw, getOATZNode(), Bytes.toBytes(newOat));
        oat = newOat;
      }

      for (String delete : toDelete)
        ZKUtil.delete(zkw, delete);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void setOAT() throws IOException {
    String oatZNode = getOATZNode();
    byte[] data = ZKUtil.getData(zkw, oatZNode);
    if (data == null) {
      data = Bytes.toBytes(oat = 0);
      ZKUtil.create(zkw, oatZNode, data, CreateMode.PERSISTENT);
    } else {
      oat = Bytes.toInt(data);
    }
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
    return ZKUtil.joinZNode(zkw.baseZNode, htx);
  }
}
