package edu.illinois.troup.tm.log;

import edu.illinois.troup.tm.Key;
import edu.illinois.troup.tm.TID;

public interface LogRecord<K extends Key> {

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

  /**
   * Group key of this log record
   * 
   * @return
   */
  K getGroupKey();
}