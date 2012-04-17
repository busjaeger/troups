package edu.illinois.htx.tsm;

import java.io.IOException;

public class NoSuchTimestampException extends IOException {

  private static final long serialVersionUID = -2764520867007596835L;

  public NoSuchTimestampException() {
    super();
  }

  public NoSuchTimestampException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoSuchTimestampException(String message) {
    super(message);
  }

  public NoSuchTimestampException(Throwable cause) {
    super(cause);
  }

}
