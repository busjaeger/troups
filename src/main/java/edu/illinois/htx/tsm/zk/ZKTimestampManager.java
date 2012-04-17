package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_TRANSACTIONS;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import edu.illinois.htx.tsm.TimestampListener;
import edu.illinois.htx.tsm.TimestampManager;
import edu.illinois.htx.tsm.TimestampState;

public class ZKTimestampManager implements TimestampManager {

  private static final char SEP = '/';
  private static final String ZK_OWNER_NODE = "owner";

  protected final ZooKeeperWatcher zkw;
  protected final String transNode;
  protected final String transDir;

  public ZKTimestampManager(ZooKeeperWatcher zkw) {
    this.zkw = zkw;
    Configuration conf = zkw.getConfiguration();
    String htx = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String trans = conf.get(ZOOKEEPER_ZNODE_TRANSACTIONS,
        DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS);
    this.transNode = join(zkw.baseZNode, htx, trans);
    this.transDir = join(transNode, "");
  }

  @Override
  public long next() throws IOException {
    try {
      /*
       * retry if we fail to create the ephemeral owner node because the
       * transaction node has been deleted: the two operations are not atomic,
       * so there is a window in between in which the timestamp collector may
       * think the transaction has failed (because the owner node is gone) and
       * therefore collect it.
       */
      while (true) {
        String tranZNode;
        try {
          tranZNode = createWithParents(transDir, PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
          throw new IOException(e);
        }
        long id = Long.parseLong(ZKUtil.getNodeName(tranZNode));
        String ownerNode = getOwnerNode(tranZNode);
        try {
          zkw.getRecoverableZooKeeper().create(ownerNode, new byte[0],
              Ids.OPEN_ACL_UNSAFE, EPHEMERAL);
          return id;
        } catch (KeeperException.NoNodeException e) {
          continue;
        } catch (KeeperException e) {
          throw new IOException(e);
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
  }

  @Override
  public void done(long ts) throws IOException {
    byte[] tranState = WritableUtils.toByteArray(new TimestampState(true));
    String tranNode = join(transNode, ts);
    try {
      try {
        ZKUtil.setData(zkw, tranNode, tranState);
      } catch (KeeperException.NoNodeException e) {
        // ignore
      }
      try {
        ZKUtil.deleteNode(zkw, getOwnerNode(tranNode));
      } catch (KeeperException.NoNodeException e) {
        // TODO
      }
    } catch (KeeperException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
  }

  @Override
  public long getLastDeletedTimestamp() {
    return 0;
  }

  @Override
  public void addTimestampListener(TimestampListener listener) {
  }

  protected String join(String base, Object... nodes) {
    StringBuilder b = new StringBuilder(base);
    for (Object node : nodes)
      b.append(SEP).append(node);
    return b.toString();
  }

  protected String getOwnerNode(String tranNode) {
    return join(tranNode, ZK_OWNER_NODE);
  }

  protected String createWithParents(String znode, CreateMode mode)
      throws KeeperException, InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      return zkw.getRecoverableZooKeeper().create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, mode);
    } catch (KeeperException.NoNodeException nne) {
      String parent = ZKUtil.getParent(znode);
      ZKUtil.createWithParents(zkw, parent);
      return createWithParents(znode, mode);
    }
  }
}
