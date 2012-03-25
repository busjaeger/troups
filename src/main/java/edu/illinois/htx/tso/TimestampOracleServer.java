package edu.illinois.htx.tso;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;

import edu.illinois.htx.tm.HTXConstants;

/**
 * A server implementation for the TimestampOracle.
 */
public class TimestampOracleServer implements Stoppable, Runnable {

  private final TimestampOracle tso;
  private final RpcServer rpcServer;
  private final ServerName serverName;
  private final ZooKeeperWatcher zooKeeper;
  private volatile boolean stopped;

  public TimestampOracleServer(Configuration conf)
      throws ZooKeeperConnectionException, IOException {
    this(conf, new ZooKeeperWatcher(conf, "TransactionManagerServer",
        new Abortable() {
          @Override
          public boolean isAborted() {
            // TODO
            return false;
          }

          @Override
          public void abort(String why, Throwable e) {
            // TODO
          }
        }));
  }

  TimestampOracleServer(Configuration conf, ZooKeeperWatcher zooKeeper)
      throws IOException {
    this.tso = new TimestampOracle();
    this.rpcServer = createRpcServer(conf, tso);
    InetSocketAddress isa = rpcServer.getListenerAddress();
    this.serverName = new ServerName(isa.getHostName(), isa.getPort(),
        System.currentTimeMillis());
    this.zooKeeper = zooKeeper;
    this.stopped = true;
  }

  static RpcServer createRpcServer(Configuration conf, TimestampOracle tm)
      throws IOException {
    String hostname = DNS.getDefaultHost(
        conf.get("hbase.master.dns.interface", "default"),
        conf.get("hbase.master.dns.nameserver", "default"));
    int port = conf.getInt(HTXConstants.TSO_PORT, HTXConstants.DEFAULT_TSO_PORT);
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null)
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    int numHandlers = conf.getInt(HTXConstants.TSO_HANDLER_COUNT,
        HTXConstants.DEFAULT_TSO_HANDLER_COUNT);
    RpcServer server = HBaseRPC.getServer(tm,
        new Class<?>[] { TimestampOracleProtocol.class },
        initialIsa.getHostName(), initialIsa.getPort(), numHandlers, 0,
        conf.getBoolean("hbase.rpc.verbose", false), conf, 0);
    return server;
  }

  void start() throws IOException {
    rpcServer.start();
    try {
      ZKUtil.createEphemeralNodeAndWatch(zooKeeper, "/hbase/tm",
          serverName.getVersionedBytes());
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    synchronized (this) {
      stopped = false;
    }
  }

  @Override
  public void stop(String why) {
    rpcServer.stop();
    synchronized (this) {
      stopped = true;
      notifyAll();
    }
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  // following methods used to run the TMS in its own VM

  @Override
  public void run() {
    try {
      start();
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    synchronized (this) {
      while (!stopped) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TimestampOracleServer tms = new TimestampOracleServer(conf);
    tms.run();
  }

}
