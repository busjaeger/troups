package edu.illinois.troups;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public interface Constants {

  // configuration properties

  public static final String CLIENT_THREAD_COUNT = "troups.client.thread.count";

  public static final int DEFAULT_CLIENT_THREAD_COUNT = 5;

  public static final String TM_THREAD_COUNT = "troups.tm.thread.count";

  public static final int DEFAULT_TM_THREAD_COUNT = 10;

  public static final String TRANSACTION_TIMEOUT = "troups.transaction.timeout";

  public static final long DEFAULT_TRANSACTION_TIMEOUT = MINUTES.toMillis(2);

  // Timestamp service configuration properties

  public static final String TSS_IMPL = "troups.tss.impl";

  public static final int TSS_IMPL_VALUE_ZOOKEEPER = 1;

  public static final int TSS_IMPL_VALUE_TABLE = 2;

  public static final int TSS_IMPL_VALUE_SERVER = 3;

  public static final int DEFAULT_TSS_IMPL = TSS_IMPL_VALUE_TABLE;

  public static final String TSS_COLLECTOR_INTERVAL = "troups.tss.collector.interval";

  public static final long DEFAULT_TSS_COLLECTOR_INTERVAL = SECONDS.toMillis(5);

  // Timestamp service htable properties

  public static final String TSS_TABLE_NAME = "troups.tss.table.name";

  public static final String DEFAULT_TSS_TABLE_NAME = "troups-tss";

  public static final String TSS_TABLE_FAMILY_NAME = "troups.tss.table.family.name";

  public static final String DEFAULT_TSS_TABLE_FAMILY_NAME = "tss";

  public static final String TSS_TIMESTAMP_TIMEOUT = "troups.tss.timestamp.timeout";

  public static final long DEFAULT_TSS_TIMESTAMP_TIMEOUT = MINUTES.toMillis(5);

  // Timestamp service zookeeper properties

  public static final String ZOOKEEPER_ZNODE_BASE = "troups.zookeeper.znode.base";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_BASE = "troups";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMPS = "troups.zookeeper.znode.timestamps";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMPS = "timestamps";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "troups.zookeeper.znode.tsrs";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "tsrs";

  public static final String ZOOKEEPER_ZNODE_LRT = "troups.zookeeper.znode.lrt";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_LRT = "lrt";

  // Log configuration properties

  // note: currently only table works
  public static final String LOG_IMPL = "troups.log.impl";

  public static final int LOG_IMPL_VALUE_TABLE = 1;

  public static final int LOG_IMPL_VALUE_FILE = 2;

  public static final int LOG_IMPL_VALUE_FAMILY = 3;

  public static final int DEFAULT_LOG_IMPL = LOG_IMPL_VALUE_TABLE;

  public static final String LOG_DISABLE_TRUNCATION = "troups.log.disable.truncation";

  public static final boolean DEFAULT_LOG_DISABLE_TRUNCATION = false;

  // log table configuration properties

  public static final String LOG_TABLE_NAME = "troups.log.table.name";

  public static final String DEFAULT_LOG_TABLE_NAME = "troups-log";

  public static final String LOG_TABLE_FAMILY_NAME = "troups.log.table.family.name";

  public static final String DEFAULT_LOG_TABLE_FAMILY_NAME = "log";

  // log family configuration properties

  public static final String LOG_FAMILY_NAME = "troups.log.family.name";

  public static final String DEFAULT_LOG_FAMILY_NAME = "log";

  // internal constants

  public static final String ATTR_NAME_TID = "troups-tid";

  public static final String ATTR_NAME_XID = "troups-xid";

  public static final String ATTR_NAME_DEL = "troups-del";

  public static final String ATTR_NAME_BEG = "troups-beg";

}