package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.Constants.DEFAULT_TM_TSC_INTERVAL;
import static edu.illinois.htx.Constants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.Constants.DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
import static edu.illinois.htx.Constants.TM_TSC_INTERVAL;
import static edu.illinois.htx.Constants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.Constants.ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
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

public class TimestampReclaimer implements Runnable {

  private final ReclaimableTimestampManager tsm;
  private final ZooKeeperWatcher zkw;
  private final ScheduledExecutorService pool;
  private final long interval;
  private final String collectorsNode;

  private String zNode;
  private Long lrt;

  public TimestampReclaimer(ReclaimableTimestampManager tsm,
      Configuration conf, ScheduledExecutorService pool, ZooKeeperWatcher zkw) {
    this.tsm = tsm;
    this.zkw = zkw;
    this.pool = pool;
    String base = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String collectors = conf.get(ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS,
        DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS);
    this.collectorsNode = join(zkw.baseZNode, base, collectors);
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
      if (lrt == null)
        lrt = tsm.getLastReclaimedTimestamp();

      long newLrt = lrt;
      List<Long> deletes = new ArrayList<Long>();
      // the order of these two calls is important: it's OK to not reclaim a
      // timestamp, but not OK to reclaim one that's still in use!
      long lct = tsm.getLastCreatedTimestamp();
      Iterable<Long> timestamps = tsm.getTimestamps();

      if (!timestamps.iterator().hasNext()) {
        newLrt = lct;
      } else {
        for (long ts : tsm.getTimestamps()) {
          // time-stamps that we failed to delete before
          if (ts < lrt) {
            deletes.add(ts);
          }
          // time-stamps after current lrt: check if still needed
          else if (ts >= lrt) {
            if (!tsm.isReleased(ts)) {
              lrt = ts - 1;
              break;
            }
            deletes.add(ts);
          }
        }
      }
      if (newLrt != lrt) {
        tsm.setLastReclaimedTimestamp(newLrt);
        lrt = newLrt;
      }

      for (Long delete : deletes)
        try {
          tsm.release(delete);
        } catch (NoSuchTimestampException e) {
          // ignore
        }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
