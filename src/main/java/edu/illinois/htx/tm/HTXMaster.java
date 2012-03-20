package edu.illinois.htx.tm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.HMasterCommandLine;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.zookeeper.KeeperException;

/**
 * Runs the TransactionManagerServer as a separate service in the HMaster VM
 */
public class HTXMaster extends HMaster {

  private final TransactionManagerServer tms;

  public HTXMaster(Configuration conf) throws IOException, KeeperException,
      InterruptedException {
    super(conf);
    try {
      this.tms = new TransactionManagerServer(conf, getZooKeeper());
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public void run() {
    System.out.println("Starting TransactionManagerServer");
    try {
      tms.start();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to start TransactionManagerServer", e);
    }
    System.out.println("Started TransactionManagerServer");
    super.run();
  }

  @Override
  public void stop(String why) {
    tms.stop(why);
    super.stop(why);
  }

  public static void main(String[] args) throws Exception {
    VersionInfo.logVersion();
    new HMasterCommandLine(HTXMaster.class).doMain(args);
  }
}
