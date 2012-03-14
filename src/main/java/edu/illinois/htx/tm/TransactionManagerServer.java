package edu.illinois.htx.tm;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.net.DNS;

/**
 * A server implementation for the TransactionManager. The server can be either
 * be run stand-alone in its own JVM or hosted in another server, such as the
 * HMaster.
 */
public class TransactionManagerServer implements Stoppable, Runnable {

  private final TransactionManager tm;
  private final RpcServer rpcServer;
  private volatile boolean stopped;

  TransactionManagerServer(Configuration conf) throws IOException {
    this.tm = new TransactionManager(this);
    this.rpcServer = createRpcServer(conf, tm);
    this.stopped = true;
  }

  static RpcServer createRpcServer(Configuration conf, TransactionManager tm)
      throws IOException {
    String hostname = DNS.getDefaultHost(
        conf.get("hbase.master.dns.interface", "default"),
        conf.get("hbase.master.dns.nameserver", "default"));
    int port = conf.getInt(HTXConstants.TM_PORT, HTXConstants.DEFAULT_TM_PORT);
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null)
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    int numHandlers = conf.getInt(HTXConstants.TM_HANDLER_COUNT,
        HTXConstants.DEFAULT_HANDLER_COUNT);
    RpcServer server = HBaseRPC.getServer(tm, new Class<?>[] {
        TransactionManagerInterface.class, VersionTracker.class },
        initialIsa.getHostName(), initialIsa.getPort(), numHandlers, 0,
        conf.getBoolean("hbase.rpc.verbose", false), conf, 0);
    return server;
  }

  void start() throws IOException {
    rpcServer.start();
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
    TransactionManagerServer tms = new TransactionManagerServer(conf);
    Thread t = new Thread(tms, "TransactionManagerServer");
    t.setDaemon(true);
    t.start();
  }

}
