package edu.illinois.troups.tsm;

import static edu.illinois.troups.Constants.DEFAULT_TSS_COLLECTOR_INTERVAL;
import static edu.illinois.troups.Constants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.troups.Constants.DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
import static edu.illinois.troups.Constants.TSS_COLLECTOR_INTERVAL;
import static edu.illinois.troups.Constants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.troups.Constants.ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS;
import static edu.illinois.troups.tsm.zk.Util.createWithParents;
import static edu.illinois.troups.tsm.zk.Util.join;
import static edu.illinois.troups.tsm.zk.Util.setWatch;
import static edu.illinois.troups.tsm.zk.Util.toDir;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class TimestampReclaimer implements Runnable {

  private static final Log LOG = LogFactory.getLog(TimestampReclaimer.class);

  private final Runnable reclaimer;
  private final ZooKeeperWatcher zkw;
  private final ScheduledExecutorService pool;
  private final long interval;
  private final String collectorsNode;

  private String zNode;

  public TimestampReclaimer(Runnable reclaimer, Configuration conf,
      ScheduledExecutorService pool, ZooKeeperWatcher zkw) {
    this.reclaimer = reclaimer;
    this.zkw = zkw;
    this.pool = pool;
    String base = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String collectors = conf.get(ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS,
        DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS);
    this.collectorsNode = join(zkw.baseZNode, base, collectors);
    this.interval = conf.getLong(TSS_COLLECTOR_INTERVAL,
        DEFAULT_TSS_COLLECTOR_INTERVAL);
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
      LOG.info("Collector leader");
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
              e.printStackTrace(System.out);
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
    reclaimer.run();
  }

}
