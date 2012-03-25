package edu.illinois.htx.tso;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.ipc.ProtocolSignature;

/**
 * TODO:
 * <ol>
 * <li>recover from server restart or crash
 * </ol>
 */
class TimestampOracle implements TimestampOracleProtocol {

  private AtomicLong current = new AtomicLong();

  @Override
  public long next() {
    return current.incrementAndGet();
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
