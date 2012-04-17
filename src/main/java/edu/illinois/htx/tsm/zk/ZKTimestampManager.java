package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_LDT;
import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_BASE;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_LDT;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_TRANSACTIONS;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.TimestampManager;
import edu.illinois.htx.tsm.TimestampState;

public class ZKTimestampManager implements TimestampManager {

  private static final char SEP = '/';
  private static final String ZK_OWNER_NODE = "owner";

  protected final ZooKeeperWatcher zkw;
  protected final String transNode;
  protected final String transDir;
  protected final String ldtNode;

  public ZKTimestampManager(ZooKeeperWatcher zkw) {
    this.zkw = zkw;
    Configuration conf = zkw.getConfiguration();
    String htx = conf.get(ZOOKEEPER_ZNODE_BASE, DEFAULT_ZOOKEEPER_ZNODE_BASE);
    String trans = conf.get(ZOOKEEPER_ZNODE_TRANSACTIONS,
        DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS);
    String ldt = conf.get(ZOOKEEPER_ZNODE_LDT, DEFAULT_ZOOKEEPER_ZNODE_LDT);
    this.transNode = join(zkw.baseZNode, htx, trans);
    this.transDir = join(transNode, "");
    this.ldtNode = join(zkw.baseZNode, htx, ldt);
  }

  @Override
  public long next() throws IOException {
    try {
      while (true) {
        byte[] tranState = WritableUtils.toByteArray(new TimestampState(false));
        String tranZNode;
        try {
          tranZNode = createWithParents(transDir, tranState,
              PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
          throw new IOException(e);
        }
        long id = getId(tranZNode);
        String ownerNode = getOwnerNode(tranZNode);
        try {
          zkw.getRecoverableZooKeeper().create(ownerNode, new byte[0],
              Ids.OPEN_ACL_UNSAFE, EPHEMERAL);
          return id;
        } catch (KeeperException.NoNodeException e) {
          /*
           * retry if we fail to create the ephemeral owner node because the
           * transaction node has been deleted: the two operations are not
           * atomic, so there is a window in between in which the time-stamp
           * collector may think the transaction has failed (because the owner
           * node is gone) and therefore delete it.
           */
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
    disconnect(ts);
    setState(ts, new TimestampState(true));
  }

  @Override
  public boolean isDone(long ts) throws NoSuchTimestampException, IOException {
    TimestampState state = getState(ts);
    return state.isDone() || !isConnected(ts);
  }

  @Override
  public void delete(long ts) throws NoSuchTimestampException, IOException {
    String tranNode = join(transNode, ts);
    try {
      ZKUtil.deleteNodeRecursively(zkw, tranNode);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public void setState(long ts, TimestampState state)
      throws NoSuchTimestampException, IOException {
    String tranNode = join(transNode, ts);
    byte[] tranState = WritableUtils.toByteArray(state);
    try {
      ZKUtil.setData(zkw, tranNode, tranState);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Long> getTimestamps() throws IOException {
    List<String> children;
    try {
      children = ZKUtil.listChildrenNoWatch(zkw, transNode);
    } catch (KeeperException e) {
      throw new IOException();
    }
    Collections.sort(children);
    return Iterables.transform(children, new Function<String, Long>() {
      @Override
      public Long apply(String path) {
        // TODO ZK generates integers
        return Long.parseLong(ZKUtil.getNodeName(path));
      }
    });
  }

  public void disconnect(long ts) throws IOException {
    String ownerNode = getOwnerNode(join(transNode, ts));
    try {
      ZKUtil.deleteNode(zkw, ownerNode);
    } catch (KeeperException.NoNodeException e) {
      // node has already been disconnected
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public boolean isConnected(long ts) throws IOException {
    String ownerNode = getOwnerNode(join(transNode, ts));
    try {
      return ZKUtil.checkExists(zkw, ownerNode) != -1;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public TimestampState getState(long ts) throws NoSuchTimestampException,
      IOException {
    String tranNode = join(transNode, ts);
    try {
      byte[] data = ZKUtil.getData(zkw, tranNode);
      return new TimestampState(data);
    } catch (NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getLastDeletedTimestamp() throws IOException {
    try {
      byte[] data = ZKUtil.getData(zkw, ldtNode);
      return data == null ? 0 : Bytes.toInt(data);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setLastDeletedTimestamp(long ts) throws IOException {
    byte[] data = Bytes.toBytes(ts);
    try {
      try {
        ZKUtil.setData(zkw, ldtNode, data);
      } catch (NoNodeException e) {
        try {
          createWithParents(ldtNode, data, CreateMode.PERSISTENT);
        } catch (NodeExistsException e1) {
          setLastDeletedTimestamp(ts);
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e1) {
      Thread.interrupted();
      throw new IOException(e1);
    }
  }

  @Override
  public void addLastDeletedTimestampListener(TimestampListener listener) {
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

  protected long getId(String node) {
    return Long.parseLong(ZKUtil.getNodeName(node));
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

}
