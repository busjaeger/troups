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

  static String create(ZooKeeperWatcher zkw, String znode, byte[] data,
      CreateMode mode) throws InterruptedException, KeeperException {
    ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
    return zkw.getRecoverableZooKeeper().getZooKeeper()
        .create(znode, data, Ids.OPEN_ACL_UNSAFE, mode);
  }

  static String createWithParents(ZooKeeperWatcher zkw, String znode,
      byte[] data, CreateMode mode) throws KeeperException,
      InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().getZooKeeper()
          .create(znode, data, Ids.OPEN_ACL_UNSAFE, mode);
    } catch (KeeperException.NoNodeException nne) {
      String parent = ZKUtil.getParent(znode);
      createWithParents(zkw, parent, new byte[0], CreateMode.PERSISTENT);
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

  static String join(String base, long id) {
    return join(base, fromId(id));
  }

  static String join(String base, long id, long id2) {
    return join(base, fromId(id), fromId(id2));
  }

  static String join(String base, String path) {
    return base + ZK_SEP + path;
  }

  static String join(String base, String path, String path2) {
    return base + ZK_SEP + path + ZK_SEP + path2;
  }

  static String toDir(String node) {
    return new StringBuilder(node.length() + 1).append(node).append(ZK_SEP)
        .toString();
  }

  static long getId(String node) {
    String nodeName = ZKUtil.getNodeName(node);
    String id = nodeName.substring(nodeName.length() - 10, nodeName.length());
    return Long.parseLong(id);
  }

  static String fromId(long id) {
    String path = String.valueOf(id);
    int padding = 10 - path.length();
    if (padding > 0) {
      char[] cs = new char[10];
      int i = 0;
      for (; i < padding; i++)
        cs[i] = '0';
      for (; i < 10; i++)
        cs[i] = path.charAt(i - padding);
      path = new String(cs);
    }
    return path;
  }

}
