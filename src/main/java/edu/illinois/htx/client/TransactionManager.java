package edu.illinois.htx.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.tm.TransactionAbortedException;

public class TransactionManager implements Closeable {

  private final HTXConnection connection;

  public TransactionManager(Configuration conf) throws IOException {
    this.connection = HTXConnectionManager.getConnection(conf);
  }

  public Transaction begin() throws IOException {
    long tts = connection.getTransactionManager().begin();
    return new Transaction(tts, this);
  }

  public void rollback(Transaction ta) throws IOException {
    connection.getTransactionManager().abort(ta.ts);
  }

  public void commit(Transaction ta) throws TransactionAbortedException,
      IOException {
    connection.getTransactionManager().commit(ta.ts);
  }

  @Override
  public void close() throws IOException {
    connection.close();
  }

}
