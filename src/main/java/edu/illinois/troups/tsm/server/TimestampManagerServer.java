package edu.illinois.troups.tsm.server;

import org.apache.hadoop.hbase.ipc.VersionedProtocol;

import edu.illinois.troups.tsm.SharedTimestampManager;

public interface TimestampManagerServer extends VersionedProtocol,
    SharedTimestampManager {

  public static final long VERSION = 1L;

  
}
