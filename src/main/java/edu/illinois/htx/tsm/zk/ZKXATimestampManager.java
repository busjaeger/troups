package edu.illinois.htx.tsm.zk;

import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import edu.illinois.htx.tsm.XATimestampManager;

public class ZKXATimestampManager extends ZKTimestampManager implements
    XATimestampManager {

  public ZKXATimestampManager(ZooKeeperWatcher zkw) {
    super(zkw);
  }

  @Override
  public long join(long ts) {
    return 0;
  }

  @Override
  public void prepared(long ts, long pid) {
  }

  @Override
  public void done(long ts, long pid) {
  }

  @Override
  public boolean addListener(long ts, long pid, ParticipantListener listener)
      throws IOException {
    String partNode = join(transNode, ts, pid);
    try {
      return setWatch(partNode, new ParticipantWatcher(listener));
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new IOException(e);
    }
  }

  @Override
  public boolean addListener(long ts, CoordinatorListener listener) {
    return false;
  }

  protected boolean setWatch(String znode, Watcher watcher)
      throws KeeperException, InterruptedException {
    try {
      ZKUtil.waitForZKConnectionIfAuthenticating(zkw);
      zkw.getRecoverableZooKeeper().getData(znode, watcher, null);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return false;
    }
  }

  private static class ParticipantWatcher implements Watcher {
    private final ParticipantListener listener;

    ParticipantWatcher(ParticipantListener listener) {
      this.listener = listener;
    }

    @Override
    public void process(WatchedEvent event) {
      
    }
  }
}
