package edu.illinois.htx.tm;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class TransactionAbortedException extends DoNotRetryIOException {

  private static final long serialVersionUID = -7646035544053931902L;

  public TransactionAbortedException() {
    super();
  }

  public TransactionAbortedException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransactionAbortedException(String message) {
    super(message);
  }

  public TransactionAbortedException(Throwable cause) {
    super(null, cause);
  }

}
