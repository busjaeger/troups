package edu.illinois.troups;

import static java.util.concurrent.TimeUnit.SECONDS;

public interface Constants {

  // configuration properties

  public static final String CLIENT_THREAD_COUNT = "troups.tm.client.thread.count";

  public static final int DEFAULT_CLIENT_THREAD_COUNT = 5;

  public static final String TM_THREAD_COUNT = "troups.tm.thread.count";

  public static final int DEFAULT_TM_THREAD_COUNT = 10;

  public static final String TSC_INTERVAL = "troups.tm.tsc.interval";

  public static final long DEFAULT_TSC_INTERVAL = 5000l;

  public static final String TRANSACTION_TIMEOUT = "troups.transaction.timeout";
  
  public static final long DEFAULT_TRANSACTION_TIMEOUT = SECONDS.toMillis(180);

  

  // Log table schema

//  public static final String TM_LOG_FAMILY_NAME = "troups.tm.log.family.name";

  public static final String LOG_IMPL = "troups.tm.log.impl";

  public static final String DEFAULT_LOG_IMPL = "table";

  public static final String LOG_TABLE_NAME = "troups.tm.log.table.name";

  public static final String DEFAULT_LOG_TABLE_NAME = "troups";

  public static final String LOG_TABLE_FAMILY_NAME = "troups.tm.log.table.family.name";

  public static final String DEFAULT_LOG_TABLE_FAMILY_NAME = "log";

  public static final String LOG_DISABLE_TRUNCATION = "troups.tm.log.disable.truncation";

  public static final boolean DEFAULT_LOG_DISABLE_TRUNCATION = false;

  // ZooKeeper nodes

  public static final String ZOOKEEPER_ZNODE_BASE = "zookeeper.znode.base";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_BASE = "troups";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMPS = "zookeeper.znode.timestamps";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMPS = "timestamps";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "zookeeper.znode.tsrs";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "tsrs";

  public static final String ZOOKEEPER_ZNODE_LRT = "zookeeper.znode.lrt";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_LRT = "lrt";

  // internal constants

  public static final String ATTR_NAME_TID = "troups-tid";

  public static final String ATTR_NAME_XID = "troups-xid";

  public static final String ATTR_NAME_DEL = "troups-del";

  public static final String ATTR_NAME_BEG = "troups-beg";

}