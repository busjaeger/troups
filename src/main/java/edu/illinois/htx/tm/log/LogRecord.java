package edu.illinois.htx.tm.log;


public interface LogRecord {

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
   * type of log record
   * 
   * @return
   */
  int getType();

}