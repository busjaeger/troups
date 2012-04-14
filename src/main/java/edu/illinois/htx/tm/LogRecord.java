package edu.illinois.htx.tm;

public interface LogRecord<K> {

  public enum Type {
    BEGIN, READ, WRITE, DELETE, COMMIT, ABORT, FINALIZE
  }

  LogRecord.Type getType();

  /**
   * log sequence number
   * 
   * @return
   */
  long getSID();

  /**
   * transaction ID
   * 
   * @return
   */
  long getTID();

  /**
   * key touched by operation
   * 
   * @return
   */
  K getKey();

  /**
   * version read
   * 
   * @return
   */
  long getVersion();

}