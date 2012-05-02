package edu.illinois.troups.tm;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class NotEnoughVersionsException extends DoNotRetryIOException {

  private static final long serialVersionUID = 4097842066929261901L;

  public NotEnoughVersionsException() {
    super();
  }

  public NotEnoughVersionsException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotEnoughVersionsException(String message) {
    super(message);
  }

}
