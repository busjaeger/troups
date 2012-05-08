package edu.illinois.troups.tsm.server;

import static edu.illinois.troups.Constants.DEFAULT_TSS_SERVER_PORT;
import static edu.illinois.troups.Constants.TSS_SERVER_NAME;
import static edu.illinois.troups.Constants.TSS_SERVER_PORT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.HBaseRPC;

import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.NotOwnerException;
import edu.illinois.troups.tsm.SharedTimestampManager;

public class TimestampManagerServerClient implements SharedTimestampManager {

  private static final Log LOG = LogFactory
      .getLog(TimestampManagerServerClient.class);

  private final TimestampManagerServer proxy;
  private final ScheduledExecutorService pool;
  private final List<TimestampReclamationListener> listeners;
  private final AtomicLong lastReclaimed;

  public TimestampManagerServerClient(Configuration conf,
      ScheduledExecutorService pool) throws IOException {
    String host = conf.get(TSS_SERVER_NAME);
    if (host == null)
      throw new IllegalStateException(TSS_SERVER_NAME + " property not set");
    int port = conf.getInt(TSS_SERVER_PORT, DEFAULT_TSS_SERVER_PORT);
    InetSocketAddress isa = new InetSocketAddress(host, port);
    LOG.info("TSM client connecting to: " + isa);
    int rpcTimeout = conf.getInt(HBASE_RPC_TIMEOUT_KEY,
        DEFAULT_HBASE_RPC_TIMEOUT);
    this.proxy = (TimestampManagerServer) HBaseRPC.getProxy(
        TimestampManagerServer.class, TimestampManagerServer.VERSION, isa,
        conf, rpcTimeout);
    this.lastReclaimed = new AtomicLong(proxy.getLastReclaimedTimestamp());
    this.listeners = new ArrayList<TimestampReclamationListener>();
    this.pool = pool;
  }

  public void addTimestampReclamationListener(
      TimestampReclamationListener listener) {
    synchronized (listeners) {
      if (listeners.isEmpty()) {
        pool.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            long newLastReclaimed;
            try {
              newLastReclaimed = proxy.getLastReclaimedTimestamp();
            } catch (IOException e) {
              e.printStackTrace(System.err);
              LOG.error("failed to retrieve last reclaimed", e);
              return;
            }
            if (newLastReclaimed != lastReclaimed.get()) {
              lastReclaimed.set(newLastReclaimed);
              for (TimestampReclamationListener listener : listeners)
                listener.reclaimed(newLastReclaimed);
            }
          }
        }, 0, 5, TimeUnit.SECONDS);// TODO make configurable
      }
      listeners.add(listener);
    }
  }

  public long getLastReclaimedTimestamp() throws IOException {
    return lastReclaimed.get();
  }

  public long acquireShared() throws IOException {
    return proxy.acquireShared();
  }

  public long acquire() throws IOException {
    return proxy.acquire();
  }

  public boolean releaseShared(long ts) throws IOException {
    return proxy.releaseShared(ts);
  }

  public long acquireReference(long ts) throws NoSuchTimestampException,
      IOException {
    return proxy.acquireReference(ts);
  }

  public boolean release(long ts) throws IOException {
    return proxy.release(ts);
  }

  public boolean releaseReference(long ts, long rid) throws IOException {
    return proxy.releaseReference(ts, rid);
  }

  public boolean isReferencePersisted(long ts, long rid)
      throws NoSuchTimestampException, IOException {
    return proxy.isReferencePersisted(ts, rid);
  }

  public void persistReferences(long ts, Iterable<Long> rids)
      throws NotOwnerException, IOException {
    proxy.persistReferences(ts, rids);
  }

  // TODO figure out how to make this method invocable via RPC
  public int compare(Long o1, Long o2) {
    return o1.compareTo(o2);
  }

}
