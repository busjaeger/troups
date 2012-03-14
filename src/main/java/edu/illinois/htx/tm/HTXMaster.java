package edu.illinois.htx.tm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.zookeeper.KeeperException;

/**
 * Runs the TransactionManagerServer as a separate service in the HMaster VM
 */
public class HTXMaster extends HMaster {

  private final TransactionManagerServer tms;

  public HTXMaster(Configuration conf) throws IOException, KeeperException,
      InterruptedException {
    super(conf);
    this.tms = new TransactionManagerServer(conf);
  }

  @Override
  public void run() {
    try {
      tms.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start TransactionManagerServer", e);
    }
    super.run();
  }

  @Override
  public void stop(String why) {
    tms.stop(why);
    super.stop(why);
  }
}
