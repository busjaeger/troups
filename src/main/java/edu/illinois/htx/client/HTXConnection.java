package edu.illinois.htx.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.client.HConnection;

import edu.illinois.htx.tm.TransactionManagerInterface;
import edu.illinois.htx.tm.VersionTracker;

public interface HTXConnection extends Closeable, Abortable {

  HConnection getHConnection();

  TransactionManagerInterface getTransactionManager() throws IOException;

  VersionTracker getVersionTracker() throws IOException;

}
