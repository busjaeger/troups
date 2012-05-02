package edu.illinois.troups.tm.log;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;

import edu.illinois.troups.tm.Key;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.TransactionLog.Record;

public interface TransactionLog<K extends Key, R extends Record<K>> extends
    Comparator<Long> {

  public static final int RECORD_TYPE_STATE_TRANSITION = 1;
  public static final int RECORD_TYPE_GET = 2;
  public static final int RECORD_TYPE_PUT = 3;

  public interface Record<K extends Key> {

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

  public interface StateTransitionRecord<K extends Key> extends Record<K> {
    /**
     * transaction state
     * 
     * @return
     */
    int getTransactionState();
  }

  public interface OperationRecord<K extends Key> extends Record<K> {
    /**
     * Key being accessed by operation
     * 
     * @return
     */
    List<K> getKeys();
  }

  public interface PutRecord<K extends Key> extends OperationRecord<K> {
    // marker interface
  }

  public interface GetRecord<K extends Key> extends OperationRecord<K> {
    /**
     * version read
     * 
     * @return
     */
    List<Long> getVersions();
  }

  public long appendStateTransition(TID tid, int state) throws IOException;

  public long appendGet(TID tid, List<K> keys, List<Long> version)
      throws IOException;

  public long appendPut(TID tid, List<K> keys) throws IOException;

  public abstract void truncate(long sid) throws IOException;

  public abstract NavigableMap<Long, R> open() throws IOException;

}
