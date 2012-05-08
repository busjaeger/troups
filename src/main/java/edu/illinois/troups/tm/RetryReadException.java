package edu.illinois.troups.tm;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class RetryReadException extends DoNotRetryIOException {

  private static final long serialVersionUID = -315007630978250360L;

  public RetryReadException() {
    super();
  }

  public RetryReadException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetryReadException(String message) {
    super(message);
  }

}
