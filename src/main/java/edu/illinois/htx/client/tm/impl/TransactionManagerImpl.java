package edu.illinois.htx.client.tm.impl;

import static edu.illinois.htx.HTXConstants.DEFAULT_TM_CLIENT_THREAD_COUNT;
import static edu.illinois.htx.HTXConstants.TM_CLIENT_THREAD_COUNT;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import edu.illinois.htx.client.tm.Transaction;
import edu.illinois.htx.client.tm.TransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.SharedTimestampManager;
import edu.illinois.htx.tsm.zk.ZKSharedTimestampManager;

public class TransactionManagerImpl extends TransactionManager {

  private final SharedTimestampManager stsm;
  private final ExecutorService pool;

  public TransactionManagerImpl(Configuration conf) throws IOException {
    HConnection connection = HConnectionManager.getConnection(conf);
    @SuppressWarnings("deprecation")
    ZooKeeperWatcher zkw = connection.getZooKeeperWatcher();
    this.stsm = new ZKSharedTimestampManager(zkw);
    int numThreads = conf.getInt(TM_CLIENT_THREAD_COUNT,
        DEFAULT_TM_CLIENT_THREAD_COUNT);
    this.pool = Executors.newFixedThreadPool(numThreads);
  }

  public Transaction begin() {
    return new LocalTransaction();
  }

  public Transaction beginXG() {
    XGTransaction tran = new XGTransaction(stsm, pool);
    try {
      tran.begin();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tran;
  }

  public void rollback(Transaction ta) {
    ta.rollback();
  }

  public void commit(Transaction ta) throws TransactionAbortedException {
    ta.commit();
  }

}
