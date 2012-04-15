package edu.illinois.htx.client.impl;

import static edu.illinois.htx.HTXConstants.DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS;
import static edu.illinois.htx.HTXConstants.ZOOKEEPER_ZNODE_TRANSACTIONS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.client.TransactionManager;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.util.ZKUtil;

public class TransactionManagerImpl extends TransactionManager {

  private final String transZNode;
  private final String transDir;
  private final HConnection connection;
  private final ZooKeeperWatcher zkw;

  @SuppressWarnings("deprecation")
  public TransactionManagerImpl(Configuration conf) throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    String trans = conf.get(ZOOKEEPER_ZNODE_TRANSACTIONS,
        DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS);
    this.zkw = connection.getZooKeeperWatcher();
    this.transZNode = ZKUtil.joinZNode(zkw.baseZNode, trans);
    this.transDir = ZKUtil.appendSeparator(transZNode);
  }

  public Transaction begin() {
    try {
      String tranZNode = ZKUtil.createSequentialWithParent(zkw, transDir);
      long id = Long.parseLong(ZKUtil.getNodeName(tranZNode));
      String clientNode = ZKUtil.createEphemeral(zkw,
          ZKUtil.joinZNode(tranZNode, "client"));
      return new LocalTransaction(id, zkw, tranZNode, clientNode);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create transaction", e);
    }
  }

  public void rollback(Transaction ta) {
    ta.rollback();
  }

  public void commit(Transaction ta) throws TransactionAbortedException {
    ta.commit();
  }

}
