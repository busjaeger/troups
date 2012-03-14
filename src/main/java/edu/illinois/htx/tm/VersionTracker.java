package edu.illinois.htx.tm;

import org.apache.hadoop.hbase.ipc.VersionedProtocol;

/**
 * TODO consider passing in batches instead of single keys to reduce RPCs
 */
public interface VersionTracker extends VersionedProtocol {

  public static final long VERSION = 1L;

  long selectReadVersion(long tts, Key key);

  void written(long tts, Key key) throws TransactionAbortedException;

  void deleted(long tts, Key key) throws TransactionAbortedException;

}
