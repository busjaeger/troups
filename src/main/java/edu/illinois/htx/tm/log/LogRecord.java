package edu.illinois.htx.tm.log;

import org.apache.hadoop.io.Writable;

public interface LogRecord extends Writable {

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