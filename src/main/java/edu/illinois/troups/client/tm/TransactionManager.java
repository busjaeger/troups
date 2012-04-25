package edu.illinois.troups.client.tm;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.troups.client.tm.impl.TransactionManagerClient;
import edu.illinois.troups.tm.TransactionAbortedException;

public abstract class TransactionManager implements Closeable {

  // could cache instances
  public static TransactionManager get(Configuration conf) throws IOException {
    return new TransactionManagerClient(conf);
  }

  public abstract Transaction begin();

  public abstract Transaction beginXG();

  public abstract void rollback(Transaction ta);

  public abstract void commit(Transaction ta)
      throws TransactionAbortedException;

}
