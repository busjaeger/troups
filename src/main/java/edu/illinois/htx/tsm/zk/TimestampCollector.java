package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.HTXConstants.DEFAULT_TM_TSC_INTERVAL;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS;
import static edu.illinois.htx.HTXConstants.TM_TSC_INTERVAL;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_COLLECTORS;
import static edu.illinois.htx.tsm.zk.Util.join;
import static edu.illinois.htx.tsm.zk.Util.setWatch;
import static edu.illinois.htx.tsm.zk.Util.toDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.TimestampManager;

/*
 * Note: this timestamp collector implementation uses Zookeeper for leader election, but is NOT tied the Zookeeper TimestampManager implementation.
 */
public class TimestampCollector implements Runnable {

  private final TimestampManager tsm;
  private final ZooKeeperWatcher zkw;
  private final ScheduledExecutorService pool;
  private final long interval;
  private final String collectorsNode;

  private String zNode;
  private boolean ldtSet;
  private long ldt;

  public TimestampCollector(TimestampManager tsm, Configuration conf,
      ScheduledExecutorService pool, ZooKeeperWatcher zkw) {
    this.tsm = tsm;
    this.zkw = zkw;
    this.pool = pool;
    String htx = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String collectors = conf.get(ZOOKEEPER_ZNODE_COLLECTORS,
        DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS);
    this.collectorsNode = join(zkw.baseZNode, htx, collectors);
    this.interval = conf.getLong(TM_TSC_INTERVAL, DEFAULT_TM_TSC_INTERVAL);
  }

  public void start() throws IOException {
    try {
      zNode = createWithParents(toDir(collectorsNode), new byte[0],
          CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
    tryToBecomeCollector();
  }

  protected String createWithParents(String znode, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, data,
          Ids.OPEN_ACL_UNSAFE, mode);
    } catch (KeeperException.NoNodeException nne) {
      String parent = ZKUtil.getParent(znode);
      ZKUtil.createWithParents(zkw, parent);
      return createWithParents(znode, data, mode);
    }
  }

  private void tryToBecomeCollector() throws IOException {
    if (becomeCollector()) {
      pool.scheduleAtFixedRate(this, interval, interval, TimeUnit.MILLISECONDS);
    }
  }

  private boolean becomeCollector() throws IOException {
    try {
      List<String> children = ZKUtil.listChildrenNoWatch(zkw, collectorsNode);
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
        if (setWatch(zkw, preceeding, new Watcher() {
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
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
    return false;
  }

  @Override
  public void run() {
    try {
      if (!ldtSet) {
        ldt = tsm.getLastDeletedTimestamp();
        ldtSet = true;
      }

      long newLDT = ldt;
      List<Long> deletes = new ArrayList<Long>();
      for (long ts : tsm.getTimestamps()) {
        if (ts < ldt)
          deletes.add(ts);
        else if (ts >= ldt) {
          try {
            if (!tsm.isDone(ts)) {
              ldt = ts;
              break;
            }
          } catch (NoSuchTimestampException e) {
            // got deleted in the meantime somehow
            continue;
          }
          deletes.add(ts);
        }
      }

      if (newLDT != ldt) {
        tsm.setLastDeletedTimestamp(newLDT);
        ldt = newLDT;
      }

      for (Long delete : deletes)
        try {
          tsm.delete(delete);
        } catch (NoSuchTimestampException e) {
          // ignore
        }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
