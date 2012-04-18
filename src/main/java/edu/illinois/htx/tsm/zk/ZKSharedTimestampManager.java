package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.tsm.zk.Util.createWithParents;
import static edu.illinois.htx.tsm.zk.Util.getId;
import static edu.illinois.htx.tsm.zk.Util.join;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT_SEQUENTIAL;

import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.SharedTimestampManager;

public class ZKSharedTimestampManager extends ZKTimestampManager implements
    SharedTimestampManager {

  public ZKSharedTimestampManager(ZooKeeperWatcher zkw) {
    super(zkw);
  }

  @Override
  public long createShared() throws IOException {
    try {
      while (true) {
        String tranZNode;
        try {
          tranZNode = createWithParents(watcher, timestampsDir, new byte[0],
              PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
          throw new IOException(e);
        }
        long id = getId(tranZNode);
        String ownerNode = getOwnerNode(tranZNode);
        try {
          createWithParents(watcher, ownerNode, new byte[0], EPHEMERAL);
          return id;
        } catch (KeeperException.NoNodeException e) {
          /*
           * retry if we fail to create the ephemeral owner node because the
           * transaction node has been deleted: the two operations are not
           * atomic, so there is a window in between in which the time-stamp
           * collector may think the transaction is done (because there is no
           * ephemeral node underneath it) and therefore delete it.
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
  public boolean delete(long ts) throws IOException {
    String tranNode = join(timestampsNode, ts);
    try {
      ZKUtil.deleteNodeRecursively(watcher, tranNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void persistReferences(long ts, Iterable<Long> rids)
      throws NoSuchTimestampException, IOException {
    String tranNode = join(timestampsNode, ts);
    References references = new References(rids);
    byte[] data = WritableUtils.toByteArray(references);
    try {
      ZKUtil.setData(watcher, tranNode, data);
    } catch (NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean addReferenceListener(long ts, long rid,
      TimestampListener listener) throws IOException {
    return false;
  }

  @Override
  public boolean hasReferences(long ts) throws NoSuchTimestampException,
      IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      byte[] data = ZKUtil.getData(watcher, tsNode);
      // deleted node
      if (data == null)
        return false;
      // non-persistent node (shared or not shared)
      if (data.length == 0) {
        String owner = getOwnerNode(tsNode);
        return ZKUtil.checkExists(watcher, owner) == -1;
      }
      return !new References(data).getRIDs().isEmpty();
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long createReference(long ts) throws IOException {
    String tranDir = Util.join(timestampsNode, ts, "");
    try {
      String partNode = Util.create(watcher, tranDir, new byte[0],
          EPHEMERAL_SEQUENTIAL);
      return getId(partNode);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean releaseReference(long ts, long rid) throws IOException {
    String tsNode = Util.join(timestampsNode, ts);
    boolean removed = false;
    // 1. try to remove reference from persistent state
    try {
      while (true) {
        Stat stat = new Stat();
        byte[] data = ZKUtil.getDataAndWatch(watcher, tsNode, stat);
        // deleted node
        if (data == null)
          return false;
        // non-persistent node (shared or not shared)
        if (data.length == 0)
          break;
        References references = new References(data);
        if (references.getRIDs().remove(rid)) {
          data = WritableUtils.toByteArray(references);
          try {
            ZKUtil.setData(watcher, tsNode, data, stat.getVersion());
            removed = true;
            break;
          } catch (KeeperException.BadVersionException e) {
            // retry
          }
        }
      }
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    // 2. try to remove reference node
    String refNode = Util.join(tsNode, rid);
    try {
      ZKUtil.deleteNode(watcher, refNode);
    } catch (KeeperException.NoNodeException e) {
      removed |= false;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return removed;
  }

  protected String getOwnerNode(String tranNode) {
    return join(tranNode, "owner");
  }

}
