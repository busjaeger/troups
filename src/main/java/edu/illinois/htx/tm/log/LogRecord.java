package edu.illinois.htx.tm.log;

import edu.illinois.htx.tm.TID;


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
  TID getTID();

  /**
   * type of log record
   * 
   * @return
   */
  int getType();

}