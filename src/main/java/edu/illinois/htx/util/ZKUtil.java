package edu.illinois.htx.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZKUtil {

  public static String createSequentialWithParents(ZooKeeperWatcher zkw,
      String znode, CreateMode mode) throws IOException {
    try {
      org.apache.hadoop.hbase.zookeeper.ZKUtil
          .waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, mode);
    } catch (KeeperException.NoNodeException nne) {
      try {
        String parent = org.apache.hadoop.hbase.zookeeper.ZKUtil
            .getParent(znode);
        org.apache.hadoop.hbase.zookeeper.ZKUtil.createWithParents(zkw, parent);
        return createSequentialWithParents(zkw, znode, mode);
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
      throw new IOException(ie);// TODO
    }
  }

  public static void create(ZooKeeperWatcher zkw, String znode, byte[] data,
      CreateMode mode) throws IOException {
    try {
      waitForZKConnectionIfAuthenticating(zkw);
      zkw.getRecoverableZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          mode);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
      throw new IOException(ie);// TODO
    }
  }

  public static String createEphemeral(ZooKeeperWatcher zkw, String znode)
      throws IOException {
    try {
      waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
      throw new IOException(ie);// TODO
    }
  }

  public static boolean setWatch(ZooKeeperWatcher zkw, String znode,
      Watcher watcher) throws IOException {
    try {
      waitForZKConnectionIfAuthenticating(zkw);
      zkw.getRecoverableZooKeeper().getData(znode, watcher, null);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
      throw new IOException(ie);// TODO
    }
  }

  public static void delete(ZooKeeperWatcher zkw, String znode) throws IOException {
    try {
      zkw.getRecoverableZooKeeper().delete(znode, -1);
    } catch (KeeperException.NoNodeException e) {
      // ignore
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public static List<String> getChildren(ZooKeeperWatcher zkw, String znode)
      throws IOException {
    try {
      return org.apache.hadoop.hbase.zookeeper.ZKUtil.listChildrenNoWatch(zkw,
          znode);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public static byte[] getData(ZooKeeperWatcher zkw, String znode)
      throws IOException {
    try {
      return org.apache.hadoop.hbase.zookeeper.ZKUtil.getDataNoWatch(zkw,
          znode, null);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public static void setData(ZooKeeperWatcher zkw, String znode, byte[] data)
      throws IOException {
    try {
      org.apache.hadoop.hbase.zookeeper.ZKUtil.setData(zkw, znode, data);
    } catch (NoNodeException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public static void waitForZKConnectionIfAuthenticating(ZooKeeperWatcher zkw)
      throws InterruptedException {
    org.apache.hadoop.hbase.zookeeper.ZKUtil
        .waitForZKConnectionIfAuthenticating(zkw);
  }

  public static String getParent(String node) {
    return org.apache.hadoop.hbase.zookeeper.ZKUtil.getParent(node);
  }

  public static String getNodeName(String path) {
    return org.apache.hadoop.hbase.zookeeper.ZKUtil.getNodeName(path);
  }

  public static String joinZNode(String prefix, String suffix) {
    return org.apache.hadoop.hbase.zookeeper.ZKUtil.joinZNode(prefix, suffix);
  }

  public static String appendSeparator(String path) {
    return path + "/";
  }
}
