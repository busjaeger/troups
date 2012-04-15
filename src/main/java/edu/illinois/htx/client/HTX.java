package edu.illinois.htx.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.client.impl.HTXImpl;

public interface HTX {

  public static final HTX HTX = new HTXImpl();
  
  TransactionManager getTransactionManager(Configuration conf) throws IOException;

  HTXTable getTable(Configuration conf, byte[] tableName) throws IOException;

}
