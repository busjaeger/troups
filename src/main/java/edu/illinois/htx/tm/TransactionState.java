package edu.illinois.htx.tm;

public interface TransactionState {

  public static final int STARTED = 1;
  public static final int COMMITTED = 2;
  public static final int ABORTED = 3;
  public static final int FINALIZED = 4;

}
