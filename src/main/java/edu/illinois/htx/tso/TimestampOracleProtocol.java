package edu.illinois.htx.tso;

import org.apache.hadoop.hbase.ipc.VersionedProtocol;

public interface TimestampOracleProtocol extends VersionedProtocol {

  public static final long VERSION = 1L;

  long next();

}
