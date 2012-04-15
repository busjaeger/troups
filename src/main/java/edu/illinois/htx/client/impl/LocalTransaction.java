package edu.illinois.htx.client.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import edu.illinois.htx.client.Transaction;
import edu.illinois.htx.regionserver.HRegionTransactionManagerProtocol;
import edu.illinois.htx.tm.TransactionAbortedException;

// TODO think about IOException during being/abort/commit
public class LocalTransaction implements Transaction {

  private final long id;

  // TODO abstract out Zookeeper use
  private final ZooKeeperWatcher zkw;
  private final String node;
  private final String eNode;

  HTable hTable;
  byte[] row;
  boolean done;

  LocalTransaction(long id, ZooKeeperWatcher zkw, String node, String eNode) {
    this.id = id;
    this.zkw = zkw;
    this.node = node;
    this.eNode = eNode;
  }

  @Override
  public long getID() {
    return id;
  }

  /**
   * 
   * @param hTable
   * @param row
   * @return true if this is the first to enlist
   */
  public void enlist(HTable table, byte[] row) {
    if (done)
      throw new IllegalStateException("Already completed");

    if (this.hTable == null) {
      this.hTable = table;
      this.row = row;
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      try {
        tm.begin(id);
      } catch (IOException e) {
        throw new RuntimeException("Failed to begin", e);
      }
    } else if (!Bytes.equals(this.hTable.getTableName(), table.getTableName())) {
      throw new IllegalStateException("Local transaction cannot span tables");
    } // TODO should we verify that row is in the same row group?
  }

  public void rollback() {
    if (done)
      return;
    if (hTable != null) {
      HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
          HRegionTransactionManagerProtocol.class, row);
      try {
        tm.abort(id);
      } catch (IOException e) {
        throw new RuntimeException("Failed to rollback", e);
      }
    }
    done();
  }

  public void commit() throws TransactionAbortedException {
    if (hTable == null)
      throw new IllegalStateException("No data to commit");
    if (done)
      return;
    HRegionTransactionManagerProtocol tm = hTable.coprocessorProxy(
        HRegionTransactionManagerProtocol.class, row);
    try {
      tm.commit(id);
    } catch (IOException e) {
      throw new RuntimeException("Failed to commit", e);
    }
    done();
  }

  private void done() {
    try {
      try {
        org.apache.hadoop.hbase.zookeeper.ZKUtil.deleteNode(zkw, eNode);
      } catch (KeeperException.NoNodeException e) {
        // shouldn't happen, but doesn't matter
      }
      org.apache.hadoop.hbase.zookeeper.ZKUtil.setData(zkw, node,
          Bytes.toBytes(1));
    } catch (KeeperException e) {
      // TODO handle system errors
    }
    done = true;
  }

}
