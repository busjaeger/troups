package edu.illinois.htx.client.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.client.TransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tsm.XATimestampManager;
import edu.illinois.htx.tsm.zk.ZKXATimestampManager;

public class TransactionManagerImpl extends TransactionManager {

  private final XATimestampManager xaTsm;

  public TransactionManagerImpl(Configuration conf) throws IOException {
    HConnection connection = HConnectionManager.getConnection(conf);
    @SuppressWarnings("deprecation")
    ZooKeeperWatcher zkw = connection.getZooKeeperWatcher();
    this.xaTsm = new ZKXATimestampManager(zkw);
  }

  public Transaction begin() {
    return new LocalTransaction();
  }

  public Transaction beginXG() {
    DistributedTransaction tran = new DistributedTransaction(xaTsm);
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
