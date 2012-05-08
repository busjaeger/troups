package edu.illinois.troups.tsm.server;

import static edu.illinois.troups.Constants.DEFAULT_TSS_SERVER_BATCH_SIZE;
import static edu.illinois.troups.Constants.DEFAULT_TSS_SERVER_HANDLER_COUNT;
import static edu.illinois.troups.Constants.DEFAULT_TSS_SERVER_PORT;
import static edu.illinois.troups.Constants.DEFAULT_TSS_TABLE_FAMILY_NAME;
import static edu.illinois.troups.Constants.DEFAULT_TSS_TABLE_NAME;
import static edu.illinois.troups.Constants.DEFAULT_TSS_TIMESTAMP_TIMEOUT;
import static edu.illinois.troups.Constants.TSS_SERVER_BATCH_SIZE;
import static edu.illinois.troups.Constants.TSS_SERVER_HANDLER_COUNT;
import static edu.illinois.troups.Constants.TSS_SERVER_NAME;
import static edu.illinois.troups.Constants.TSS_SERVER_PORT;
import static edu.illinois.troups.Constants.TSS_TABLE_FAMILY_NAME;
import static edu.illinois.troups.Constants.TSS_TABLE_NAME;
import static edu.illinois.troups.Constants.TSS_TIMESTAMP_TIMEOUT;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.RpcServer;

import edu.illinois.troups.tm.region.HRegionTransactionManager;

public class TimestampServer implements Runnable {

  static RpcServer createRpcServer(Configuration conf,
      TimestampManagerServer tsm) throws IOException {
    String host = conf.get(TSS_SERVER_NAME);
    if (host == null)
      throw new IllegalStateException(TSS_SERVER_NAME + " property not set");
    int port = conf.getInt(TSS_SERVER_PORT, DEFAULT_TSS_SERVER_PORT);
    InetSocketAddress initialIsa = new InetSocketAddress(host, port);
    if (initialIsa.getAddress() == null)
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    int numHandlers = conf.getInt(TSS_SERVER_HANDLER_COUNT,
        DEFAULT_TSS_SERVER_HANDLER_COUNT);
    RpcServer server = HBaseRPC.getServer(tsm,
        new Class<?>[] { TimestampManagerServer.class },
        initialIsa.getHostName(), initialIsa.getPort(), numHandlers, 0,
        conf.getBoolean("hbase.rpc.verbose", false), conf, 0);
    return server;
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    final TimestampServer tms = new TimestampServer(conf);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        tms.notify();
      }
    });
    tms.run();
  }

  private final TimestampManagerServer tsm;
  private final ScheduledExecutorService pool;
  private final RpcServer server;

  public TimestampServer(Configuration conf) throws IOException {
    HTablePool tablePool = new HTablePool(conf, Integer.MAX_VALUE);
    byte[] tsTableName = toBytes(conf.get(TSS_TABLE_NAME,
        DEFAULT_TSS_TABLE_NAME));
    byte[] tsFamilyName = toBytes(conf.get(TSS_TABLE_FAMILY_NAME,
        DEFAULT_TSS_TABLE_FAMILY_NAME));
    HRegionTransactionManager.demandTable(conf, tsTableName, tsFamilyName);
    long tsTimeout = conf.getLong(TSS_TIMESTAMP_TIMEOUT,
        DEFAULT_TSS_TIMESTAMP_TIMEOUT);
    long batchSize = conf.getLong(TSS_SERVER_BATCH_SIZE,
        DEFAULT_TSS_SERVER_BATCH_SIZE);
    this.tsm = new SharedTimestampManagerImpl(tablePool, tsTableName,
        tsFamilyName, batchSize, tsTimeout);
    this.pool = Executors.newScheduledThreadPool(1);
    this.server = createRpcServer(conf, tsm);
  }

  @Override
  public void run() {
    server.start();
    pool.scheduleAtFixedRate((Runnable) tsm, 30, 30, TimeUnit.SECONDS);
    synchronized (this) {
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
    }
    pool.shutdown();
    server.stop();
  }
}
