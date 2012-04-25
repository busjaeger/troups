package edu.illinois.troup.tsm;

import java.io.IOException;

public class NotOwnerException extends IOException {

  private static final long serialVersionUID = -5599942508714640122L;

  public NotOwnerException() {
    super();
  }

  public NotOwnerException(String message, Throwable cause) {
    super(message, cause);
  }

  public NotOwnerException(String message) {
    super(message);
  }

  public NotOwnerException(Throwable cause) {
    super(cause);
  }

}
