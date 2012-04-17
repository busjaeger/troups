package edu.illinois.htx.tsm.zk;

import static edu.illinois.htx.tsm.ParticipantState.ACTIVE;
import static edu.illinois.htx.tsm.zk.Util.createWithParents;
import static edu.illinois.htx.tsm.zk.Util.getId;
import static edu.illinois.htx.tsm.zk.Util.setWatch;
import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.ParticipantState;
import edu.illinois.htx.tsm.TimestampState;
import edu.illinois.htx.tsm.Version;
import edu.illinois.htx.tsm.VersionMismatchException;
import edu.illinois.htx.tsm.XATimestampManager;

public class ZKXATimestampManager extends ZKTimestampManager implements
    XATimestampManager {

  public ZKXATimestampManager(ZooKeeperWatcher zkw) {
    super(zkw);
  }

  @Override
  public boolean isDone(long ts) throws NoSuchTimestampException, IOException {
    TimestampState state = getState(ts);
    if (!state.isDone()) {
      String owner = getOwnerNode("owner");
      try {
        if (ZKUtil.checkExists(watcher, owner) != -1
            || state.hasActiveParticipants()) {
          return false;
        }
      } catch (KeeperException e) {
        throw new IOException(e);
      }
    }
    return true;
  }

  @Override
  public long join(long ts) throws IOException {
    String tranDir = Util.join(transNode, ts, "");
    byte[] state = Bytes.toBytes(ACTIVE.toString());
    try {
      String partNode = createWithParents(watcher, tranDir, state,
          EPHEMERAL_SEQUENTIAL);
      return getId(partNode);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void done(long ts, long pid) throws IOException {
    disconnect(ts, pid);
  }

  public void disconnect(long ts, long pid) throws IOException {
    String partNode = Util.join(transNode, ts, pid);
    try {
      ZKUtil.deleteNode(watcher, partNode);
    } catch (KeeperException.NoNodeException e) {
      // ignore
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void prepared(long ts, long pid) throws IOException {
    setParticipantState(ts, pid, ParticipantState.PREPARED);
  }

  @Override
  public void aborted(long ts, long pid) throws IOException {
    setParticipantState(ts, pid, ParticipantState.ABORTED);
  }

  @Override
  public void committed(long ts, long pid) throws IOException {
    // first update the time-stamp record
    while (true) {
      try {
        Version version = new Version();
        TimestampState state = getState(ts, version);
        state.getOrCreateVotes().put(pid, true);
        setState(ts, state, version);
        break;
      } catch (VersionMismatchException e) {
        // retry: someone got in between
        continue;
      }
    }
    // next update participant state in case client is still listening
    setParticipantState(ts, pid, ParticipantState.COMMITTED);
  }

  public void setParticipantState(long ts, long pid, ParticipantState state)
      throws IOException {
    String partNode = Util.join(transNode, ts, pid);
    byte[] data = Bytes.toBytes(state.toString());
    try {
      ZKUtil.setData(watcher, partNode, data);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  ParticipantState getParticipantState(String partNode) throws IOException {
    try {
      byte[] data = ZKUtil.getData(watcher, partNode);
      return ParticipantState.valueOf(Bytes.toString(data));
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setState(long tid, TimestampState state, Version version)
      throws VersionMismatchException, IOException {
    String tranNode = Util.join(transNode, tid);
    byte[] data = WritableUtils.toByteArray(state);
    try {
      ZKUtil.setData(watcher, tranNode, data, version.getVersion());
    } catch (KeeperException.BadVersionException e) {
      throw new VersionMismatchException(e);
    } catch (NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public TimestampState getState(long tid, Version version) throws IOException {
    String tranNode = Util.join(transNode, tid);
    Stat stat = new Stat();
    byte[] data;
    try {
      data = ZKUtil.getDataAndWatch(watcher, tranNode, stat);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchTimestampException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    version.setVersion(stat.getVersion());
    return new TimestampState(data);
  }

  @Override
  public boolean addParticipantListener(long ts, long pid,
      ParticipantListener listener) throws IOException {
    String partNode = Util.join(transNode, ts, pid);
    try {
      return setWatch(watcher, partNode, new ParticipantWatcher(listener, pid));
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
  }

  private class ParticipantWatcher implements Watcher {
    private final ParticipantListener listener;
    private final long pid;

    ParticipantWatcher(ParticipantListener listener, long pid) {
      this.listener = listener;
      this.pid = pid;
    }

    @Override
    public void process(WatchedEvent event) {
      String partNode = event.getPath();
      switch (event.getType()) {
      case NodeDataChanged:
        ParticipantState state;
        try {
          state = getParticipantState(partNode);
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
        switch (state) {
        case ACTIVE:
          break;
        case PREPARED:
          listener.prepared(pid);
          break;
        case ABORTED:
          listener.aborted(pid);
          break;
        case COMMITTED:
          listener.committed(pid);
          break;
        }
        break;
      case NodeDeleted:
        listener.disconnected(pid);
        break;
      default:
        break;
      }
    }
  }

}
