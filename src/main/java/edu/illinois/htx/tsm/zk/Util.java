package edu.illinois.htx.tsm.zk;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public final class Util {

  private static final char ZK_SEP = '/';

  private Util() {
    super();
  }

  static String create(ZooKeeperWatcher zkw, String znode,
      byte[] data, CreateMode mode) throws InterruptedException, KeeperException {
    ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
    return zkw.getRecoverableZooKeeper().create(znode, data,
        Ids.OPEN_ACL_UNSAFE, mode);
  }
  
  static String createWithParents(ZooKeeperWatcher zkw, String znode,
      byte[] data, CreateMode mode) throws KeeperException,
      InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, data,
          Ids.OPEN_ACL_UNSAFE, mode);
    } catch (KeeperException.NoNodeException nne) {
      String parent = ZKUtil.getParent(znode);
      ZKUtil.createWithParents(zkw, parent);
      return createWithParents(zkw, znode, data, mode);
    }
  }

  static boolean setWatch(ZooKeeperWatcher zkw, String znode, Watcher watcher)
      throws KeeperException, InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      zkw.getRecoverableZooKeeper().getData(znode, watcher, null);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    }
  }

  static String join(String base, Object... nodes) {
    StringBuilder b = new StringBuilder(base);
    for (Object node : nodes)
      b.append(ZK_SEP).append(node);
    return b.toString();
  }

  static String toDir(String node) {
    return new StringBuilder(node.length() + 1).append(node).append("")
        .toString();
  }

  static long getId(String node) {
    return Long.parseLong(ZKUtil.getNodeName(node));
  }

}
