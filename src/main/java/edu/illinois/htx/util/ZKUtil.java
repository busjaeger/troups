package edu.illinois.htx.util;

import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZKUtil {

  public static String createSequentialWithParent(ZooKeeperWatcher zkw,
      String znode) throws IOException {
    try {
      org.apache.hadoop.hbase.zookeeper.ZKUtil
          .waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (KeeperException.NoNodeException nne) {
      try {
        String parent = org.apache.hadoop.hbase.zookeeper.ZKUtil
            .getParent(znode);
        zkw.getRecoverableZooKeeper().create(parent, new byte[0],
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return createSequentialWithParent(zkw, znode);
      } catch (InterruptedException ie) {
        zkw.interruptedException(ie);
        throw new IOException(ie);// TODO
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

  public static String createEphemeral(ZooKeeperWatcher zkw, String znode)
      throws IOException {
    try {
      org.apache.hadoop.hbase.zookeeper.ZKUtil
          .waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
      throw new IOException(ie);// TODO
    }
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
