package edu.illinois.troups.client.tm.impl;

import static edu.illinois.troups.Constants.DEFAULT_TM_CLIENT_THREAD_COUNT;
import static edu.illinois.troups.Constants.TM_CLIENT_THREAD_COUNT;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import edu.illinois.troups.client.tm.Transaction;
import edu.illinois.troups.client.tm.TransactionManager;
import edu.illinois.troups.tm.TransactionAbortedException;
import edu.illinois.troups.tsm.SharedTimestampManager;
import edu.illinois.troups.tsm.zk.ZKSharedTimestampManager;

public class TransactionManagerClient extends TransactionManager {

  private final SharedTimestampManager stsm;
  private final ExecutorService pool;

  public TransactionManagerClient(Configuration conf) throws IOException {
    HConnection connection = HConnectionManager.getConnection(conf);
    @SuppressWarnings("deprecation")
    ZooKeeperWatcher zkw = connection.getZooKeeperWatcher();
    this.stsm = new ZKSharedTimestampManager(zkw);
    int numThreads = conf.getInt(TM_CLIENT_THREAD_COUNT,
        DEFAULT_TM_CLIENT_THREAD_COUNT);
    this.pool = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  public Transaction begin() {
    return new GroupTransaction();
  }

  @Override
  public Transaction beginXG() {
    CrossGroupTransaction tran = new CrossGroupTransaction(stsm, pool);
    try {
      tran.begin();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tran;
  }

  @Override
  public void rollback(Transaction ta) {
    ta.rollback();
  }

  @Override
  public void commit(Transaction ta) throws TransactionAbortedException {
    ta.commit();
  }

  @Override
  public void close() throws IOException {
    pool.shutdown();
  }

}
