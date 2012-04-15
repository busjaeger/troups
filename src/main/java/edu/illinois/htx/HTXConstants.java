package edu.illinois.htx;

public interface HTXConstants {

  // internal constants

  public static final String ATTR_NAME_TID = "htx-tid";

  public static final String ATTR_NAME_DEL = "htx-del";

  public static final String ATTR_NAME_BEG = "htx-beg";

  // configuration properties

  public static final String TSO_PORT = "htx.tso.port";

  public static final int DEFAULT_TSO_PORT = 60001;

  public static final String TSO_HANDLER_COUNT = "htx.tso.handler.count";

  public static final int DEFAULT_TSO_HANDLER_COUNT = 25;

  public static final String TM_THREAD_COUNT = "htx.tm.thread.count";

  public static final int DEFAULT_TM_THREAD_COUNT = 10;

  public static final String TM_LOG_TABLE_NAME = "htx.tm.log.table.name";

  public static final String DEFAULT_TM_LOG_TABLE_NAME = "htx";

  public static final String TM_LOG_TABLE_FAMILY_NAME = "htx.tm.log.table.family.name";

  public static final String DEFAULT_TM_LOG_TABLE_FAMILY_NAME = "log";

  public static final String ZOOKEEPER_ZNODE_TRANSACTIONS = "zookeeper.znode.transactions";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_TRANSACTIONS = "transactions";

}