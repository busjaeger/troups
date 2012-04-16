package edu.illinois.htx.client.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.WritableUtils;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.regionserver.HRegionTransactionManagerProtocol;
import edu.illinois.htx.util.ZKUtil;

public abstract class AbstractTransaction implements Transaction {

  protected final long id;

  // TODO abstract out Zookeeper use
  protected final ZooKeeperWatcher zkw;
  protected final String tranNode;
  protected final String clientNode;
  protected boolean done;

  AbstractTransaction(long id, ZooKeeperWatcher zkw, String node, String eNode) {
    this.id = id;
    this.zkw = zkw;
    this.tranNode = node;
    this.clientNode = eNode;
  }

  @Override
  public long getID() {
    return id;
  }

  protected HRegionTransactionManagerProtocol getRTM(HTable table, byte[] row) {
    return table.coprocessorProxy(HRegionTransactionManagerProtocol.class, row);
  }

  protected void done() {
    byte[] tranState = WritableUtils.toByteArray(new TransactionState(true));
    try {
      ZKUtil.delete(zkw, clientNode);
      ZKUtil.setData(zkw, tranNode, tranState);
    } catch (IOException e) {
      // TODO
    }
    done = true;
  }
}
