package edu.illinois.htx.tsm;

import java.io.IOException;

public class VersionMismatchException extends IOException {

  private static final long serialVersionUID = 857815885258712472L;

  public VersionMismatchException() {
    super();
  }

  public VersionMismatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public VersionMismatchException(String message) {
    super(message);
  }

  public VersionMismatchException(Throwable cause) {
    super(cause);
  }

}
