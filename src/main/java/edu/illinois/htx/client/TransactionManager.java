package edu.illinois.htx.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tso.TimestampOracleProtocol;

public class TransactionManager {

  private final TimestampOracleProtocol tso;

  public TransactionManager(Configuration conf) throws IOException {
    this.tso = TimestampOracleClient.getClient();
  }

  public Transaction begin() throws IOException {
    long id = tso.next();
    return new Transaction(id);
  }

  public void rollback(Transaction ta) throws IOException {
    ta.rollback();
  }

  public void commit(Transaction ta) throws TransactionAbortedException,
      IOException {
    ta.commit();
  }

}
