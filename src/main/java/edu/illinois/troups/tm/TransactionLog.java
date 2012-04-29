package edu.illinois.troups.tm;

import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableMap;

import edu.illinois.troups.tm.TransactionLog.Record;

public interface TransactionLog<K extends Key, R extends Record<K>> extends
    Comparator<Long> {

  public static final int RECORD_TYPE_STATE_TRANSITION = 1;
  public static final int RECORD_TYPE_GET = 2;
  public static final int RECORD_TYPE_PUT = 3;
  public static final int RECORD_TYPE_DELETE = 4;

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
    K getKey();
  }

  public interface PutRecord<K extends Key> extends OperationRecord<K> {
    // marker interface
  }

  public interface DeleteRecord<K extends Key> extends OperationRecord<K> {
    // marker interface
  }

  public interface GetRecord<K extends Key> extends OperationRecord<K> {
    /**
     * version read
     * 
     * @return
     */
    long getVersion();
  }

  public long appendStateTransition(TID tid, int state) throws IOException;

  public long appendGet(TID tid, K key, long version) throws IOException;

  public long appendPut(TID tid, K key) throws IOException;

  public long appendDelete(TID tid, K key) throws IOException;

  public abstract void truncate(long sid) throws IOException;

  public abstract NavigableMap<Long, R> open() throws IOException;

}
