package edu.illinois.htx.client.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.client.HTX;
import edu.illinois.htx.client.HTXTable;
import edu.illinois.htx.client.TransactionManager;

public class HTXImpl implements HTX {

  @Override
  public TransactionManager getTransactionManager(Configuration conf)
      throws IOException {
    return new TransactionManagerImpl(conf);
  }

  @Override
  public HTXTable getTable(Configuration conf, byte[] tableName)
      throws IOException {
    return new HTXTableImpl(conf, tableName);
  }

}
