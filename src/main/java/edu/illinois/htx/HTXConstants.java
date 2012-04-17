package edu.illinois.htx;

public interface HTXConstants {

  // internal constants

  public static final String ATTR_NAME_TID = "htx-tid";

  public static final String ATTR_NAME_DEL = "htx-del";

  public static final String ATTR_NAME_BEG = "htx-beg";

  // configuration properties

  public static final String TM_THREAD_COUNT = "htx.tm.thread.count";

  public static final int DEFAULT_TM_THREAD_COUNT = 10;

  public static final String TM_LOG_TABLE_NAME = "htx.tm.log.table.name";

  public static final String DEFAULT_TM_LOG_TABLE_NAME = "htx";

  public static final String TM_LOG_TABLE_FAMILY_NAME = "htx.tm.log.table.family.name";

  public static final String DEFAULT_TM_LOG_TABLE_FAMILY_NAME = "log";

  public static final String TM_TSC_INTERVAL = "htx.tm.tsc.interval";

  public static final long DEFAULT_TM_TSC_INTERVAL = 5000l;

  public static final String ZOOKEEPER_ZNODE_BASE = "zookeeper.znode.base";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_BASE = "htx";

  public static final String ZOOKEEPER_ZNODE_TRANSACTIONS = "zookeeper.znode.transactions";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS = "transactions";

  public static final String ZOOKEEPER_ZNODE_COLLECTORS = "zookeeper.znode.collectors";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_COLLECTORS = "collectors";

  public static final String ZOOKEEPER_ZNODE_LDT = "zookeeper.znode.ldt";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_LDT = "ldt";

}