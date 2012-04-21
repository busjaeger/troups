package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.Constants.DEFAULT_TM_TSC_INTERVAL;
import static edu.illinois.htx.Constants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.Constants.DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
import static edu.illinois.htx.Constants.TM_TSC_INTERVAL;
import static edu.illinois.htx.Constants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.Constants.ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
import static edu.illinois.htx.tsm.zk.Util.createWithParents;
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

import edu.illinois.htx.tsm.NoSuchTimestampException;

public class TimestampReclaimer implements Runnable {

  private final ReclaimableTimestampManager tsm;
  private final ZooKeeperWatcher zkw;
  private final ScheduledExecutorService pool;
  private final long interval;
  private final String collectorsNode;

  private String zNode;
  private Long lastReclaimed;
  private long lastSeen;

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

  public void start() {
    try {
      zNode = createWithParents(zkw, toDir(collectorsNode), new byte[0],
          CreateMode.EPHEMERAL_SEQUENTIAL);
      tryToBecomeCollector();
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new RuntimeException(e);
    }
  }

  private void tryToBecomeCollector() throws KeeperException,
      InterruptedException {
    if (becomeCollector()) {
      pool.scheduleAtFixedRate(this, interval, interval, TimeUnit.MILLISECONDS);
    }
  }

  private boolean becomeCollector() throws KeeperException,
      InterruptedException {
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
            } catch (Exception e) {
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
    return false;
  }

  @Override
  public void run() {
    try {
      if (lastReclaimed == null) {
        lastReclaimed = tsm.getLastReclaimedTimestamp();
        lastSeen = lastReclaimed;
      }

      long newLastReclaimed = lastReclaimed;
      List<Long> deletes = new ArrayList<Long>();
      List<Long> timestamps = tsm.getTimestamps();

      if (timestamps.isEmpty()) {
        // with 'lastSeen' we are not guaranteed to always clean up transactions
        // right away, but we will eventually and it performs well
        newLastReclaimed = lastSeen;
      } else {
        long last = timestamps.get(timestamps.size() - 1);
        if (last > lastSeen)
          lastSeen = last;
        for (long ts : timestamps) {
          // time-stamps that we failed to delete before
          if (ts < lastReclaimed) {
            deletes.add(ts);
          }
          // time-stamps after current lrt: check if still needed
          else if (ts >= lastReclaimed) {
            if (!tsm.isReleased(ts)) {
              lastReclaimed = ts - 1;
              break;
            }
            deletes.add(ts);
          }
        }
      }
      if (newLastReclaimed != lastReclaimed) {
        tsm.setLastReclaimedTimestamp(newLastReclaimed);
        lastReclaimed = newLastReclaimed;
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
