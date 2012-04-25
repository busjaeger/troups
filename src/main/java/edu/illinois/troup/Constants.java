package edu.illinois.troup;

public interface Constants {

  // configuration properties

  public static final String TM_CLIENT_THREAD_COUNT = "troup.tm.client.thread.count";

  public static final int DEFAULT_TM_CLIENT_THREAD_COUNT = 5;

  public static final String TM_THREAD_COUNT = "troup.tm.thread.count";

  public static final int DEFAULT_TM_THREAD_COUNT = 10;

  public static final String TM_TSC_INTERVAL = "troup.tm.tsc.interval";

  public static final long DEFAULT_TM_TSC_INTERVAL = 5000l;

  // Log table schema

  public static final String TM_LOG_TABLE_NAME = "troup.tm.log.table.name";

  public static final String DEFAULT_TM_LOG_TABLE_NAME = "troup";

  public static final String TM_LOG_TABLE_FAMILY_NAME = "troup.tm.log.table.family.name";

  public static final String DEFAULT_TM_LOG_TABLE_FAMILY_NAME = "log";

  public static final String TM_LOG_DISABLE_TRUNCATION = "troup.tm.log.disable.truncation";

  public static final boolean DEFAULT_TM_LOG_DISABLE_TRUNCATION = false;

  // ZooKeeper nodes

  public static final String ZOOKEEPER_ZNODE_BASE = "zookeeper.znode.base";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_BASE = "troup";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMPS = "zookeeper.znode.timestamps";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMPS = "timestamps";

  public static final String ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "zookeeper.znode.tsrs";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TIMESTAMP_RECLAIMERS = "tsrs";

  public static final String ZOOKEEPER_ZNODE_LRT = "zookeeper.znode.lrt";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_LRT = "lrt";

  // internal constants

  public static final String ATTR_NAME_TID = "troup-tid";

  public static final String ATTR_NAME_XID = "troup-xid";

  public static final String ATTR_NAME_DEL = "troup-del";

  public static final String ATTR_NAME_BEG = "troup-beg";

}