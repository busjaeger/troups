package edu.illinois.htx.client.tm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.client.tm.impl.TransactionManagerImpl;
import edu.illinois.htx.tm.TransactionAbortedException;

public abstract class TransactionManager {

  // could cache instances
  public static TransactionManager get(Configuration conf) throws IOException {
    return new TransactionManagerImpl(conf);
  }

  public abstract Transaction begin();

  public abstract Transaction beginXG();

  public abstract void rollback(Transaction ta);

  public abstract void commit(Transaction ta)
      throws TransactionAbortedException;

}
