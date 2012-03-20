package edu.illinois.htx.tm;

import org.apache.hadoop.hbase.ipc.VersionedProtocol;

/**
 * TODO consider passing in batches instead of single keys to reduce RPCs
 * 
 * Note: the interface declares primitive parameters, because HBase's RPC
 * mechanism has hard-coded supported classes, so {@link Key} cannot be used.
 */
public interface VersionTracker extends VersionedProtocol {

  public static final long VERSION = 1L;

  long selectReadVersion(long tts, byte[] table, byte[] row, byte[] family,
      byte[] qualifier);

  void written(long tts, byte[] table, byte[] row, byte[] family,
      byte[] qualifier) throws TransactionAbortedException;

  void deleted(long tts, byte[] table, byte[] row, byte[] family,
      byte[] qualifier) throws TransactionAbortedException;

}
