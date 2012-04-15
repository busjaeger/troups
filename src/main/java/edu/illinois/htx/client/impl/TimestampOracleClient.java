package edu.illinois.htx.client.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.ipc.ProtocolSignature;

import edu.illinois.htx.tso.TimestampOracleProtocol;

/**
 * TODO: implement connection to TimestampOracleServer
 */
public class TimestampOracleClient implements TimestampOracleProtocol {

  public static TimestampOracleClient getClient() {
    return new TimestampOracleClient();
  }

  private final AtomicLong ts = new AtomicLong();

  @Override
  public long next() {
    return ts.getAndIncrement();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return TimestampOracleProtocol.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }
}
