package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.tsm.zk.Util.create;
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.NotOwnerException;
import edu.illinois.htx.tsm.SharedTimestampManager;

public class ZKSharedTimestampManager extends ZKTimestampManager implements
    SharedTimestampManager {

  public ZKSharedTimestampManager(ZooKeeperWatcher zkw) {
    super(zkw);
  }

  // override to create base and child node
  @Override
  public long acquireShared() throws IOException {
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

  // override to delete recursively
  @Override
  public boolean release(long ts) throws IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      try {
        return ZKUtil.deleteNode(watcher, tsNode, -1);
      } catch (KeeperException.NotEmptyException e) {
        //
      }
      ZKUtil.deleteNodeRecursively(watcher, tsNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  // override to check owner node and references
  @Override
  public boolean isReleased(long ts) throws IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(watcher, tsNode, stat);
      // if node doesn't exist, it's released
      if (data == null)
        return true;
      // if node exists and is ephemeral, it has not been released
      if (stat.getEphemeralOwner() != 0)
        return false;
      // if owner node exists, the node is not released
      String owner = getOwnerNode(tsNode);
      if (ZKUtil.checkExists(watcher, owner) != -1)
        return false;
      // otherwise check if persisted references present
      if (data.length == 0)
        return true;
      return new References(data).getRIDs().isEmpty();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isHeldByCaller(long ts) throws NoSuchTimestampException,
      IOException {
    String tsNode = join(timestampsNode, ts);
    String ownerNode = getOwnerNode(tsNode);
    return isHeldByCaller(ownerNode);
  }

  // override to register listener on owner node and handle persistence logic
  @Override
  public boolean addTimestampListener(final long ts,
      final TimestampListener listener) throws IOException {
    String tsNode = join(timestampsNode, ts);
    String ownerNode = getOwnerNode(tsNode);
    return addTimestampListener(ownerNode, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
        case NodeDeleted:
          try {
            if (!isPersistent(ts))
              listener.released(ts);
          } catch (IOException e) {
            e.printStackTrace();
            // ignore
          }
        default:
          break;
        }
      }
    });
  }

  @Override
  public void persistReferences(long ts, Iterable<Long> rids)
      throws NoSuchTimestampException, IOException {
    String tsNode = join(timestampsNode, ts);

    // check if this call is issued by the owner
    if (!isHeldByCaller(ts))
      throw new NotOwnerException();

    // now persist references
    References references = new References(rids);
    byte[] data = WritableUtils.toByteArray(references);
    try {
      ZKUtil.setData(watcher, tsNode, data);
    } catch (NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  // a timestamp is persisted if it exists and has data stored in it
  private boolean isPersistent(long ts) throws IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      byte[] data = ZKUtil.getData(watcher, tsNode);
      return data != null && data.length > 0;
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean addReferenceListener(long ts, long rid,
      TimestampListener listener) throws IOException {
    String refNode = Util.join(timestampsNode, ts, rid);
    return addTimestampListener(rid, refNode, listener);
  }

  @Override
  public long acquireReference(long ts) throws IOException {
    String tsNode = Util.join(timestampsNode, ts);
    String tsDir = Util.toDir(tsNode);
    try {
      String partNode = create(watcher, tsDir, new byte[0],
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
      return false;
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

  @Override
  public boolean isReferenceHeldByMe(long ts, long rid)
      throws NoSuchTimestampException, IOException {
    String tsNode = join(timestampsNode, ts);
    String refNode = join(tsNode, rid);
    return isHeldByCaller(refNode);
  }

  @Override
  public boolean isReferencePersisted(long ts, long rid)
      throws NoSuchTimestampException, IOException {
    String tsNode = join(timestampsNode, ts);
    try {
      byte[] data = ZKUtil.getData(watcher, tsNode);
      if (data == null)
        throw new NoSuchTimestampException(String.valueOf(ts));
      if (data.length == 0)
        return false;
      return new References(data).getRIDs().contains(rid);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  protected String getOwnerNode(String tsNode) {
    return join(tsNode, "o");
  }

}
