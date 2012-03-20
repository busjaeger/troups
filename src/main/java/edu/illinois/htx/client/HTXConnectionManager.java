package edu.illinois.htx.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import edu.illinois.htx.tm.TransactionManagerInterface;
import edu.illinois.htx.tm.VersionTracker;

public class HTXConnectionManager {

  private static final Map<HConnectionKey, HTXConnection> connections = new HashMap<HConnectionKey, HTXConnection>();

  public static HTXConnection getConnection(Configuration conf)
      throws IOException {
    HConnectionKey key = new HConnectionKey(conf);
    HTXConnection connection;
    synchronized (connections) {
      connection = connections.get(key);
      if (connection == null)
        connections
            .put(key, connection = new HTXConnectionImplementation(conf));
    }
    return connection;
  }

  // copy of org.apache.hadoop.hbase.client.HConnectionManager.HConnectionKey
  /**
   * Denotes a unique key to a {@link HConnection} instance.
   * 
   * In essence, this class captures the properties in {@link Configuration}
   * that may be used in the process of establishing a connection. In light of
   * that, if any new such properties are introduced into the mix, they must be
   * added to the {@link HConnectionKey#properties} list.
   * 
   */
  static class HConnectionKey {
    public static String[] CONNECTION_PROPERTIES = new String[] {
        HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.ZOOKEEPER_CLIENT_PORT,
        HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
        HConstants.HBASE_CLIENT_PAUSE, HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.HBASE_CLIENT_PREFETCH_LIMIT,
        HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.HBASE_CLIENT_INSTANCE_ID };

    private Map<String, String> properties;
    private String username;

    public HConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<String, String>();
      if (conf != null) {
        for (String property : CONNECTION_PROPERTIES) {
          String value = conf.get(property);
          if (value != null) {
            m.put(property, value);
          }
        }
      }
      this.properties = Collections.unmodifiableMap(m);

      try {
        User currentUser = User.getCurrent();
        if (currentUser != null) {
          username = currentUser.getName();
        }
      } catch (IOException ioe) {
        // LOG.warn(
        // "Error obtaining current user, skipping username in HConnectionKey",
        // ioe);
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (username != null) {
        result = username.hashCode();
      }
      for (String property : CONNECTION_PROPERTIES) {
        String value = properties.get(property);
        if (value != null) {
          result = prime * result + value.hashCode();
        }
      }

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HConnectionKey that = (HConnectionKey) obj;
      if (this.username != null && !this.username.equals(that.username)) {
        return false;
      } else if (this.username == null && that.username != null) {
        return false;
      }
      if (this.properties == null) {
        if (that.properties != null) {
          return false;
        }
      } else {
        if (that.properties == null) {
          return false;
        }
        for (String property : CONNECTION_PROPERTIES) {
          String thisValue = this.properties.get(property);
          String thatValue = that.properties.get(property);
          if (thisValue == thatValue) {
            continue;
          }
          if (thisValue == null || !thisValue.equals(thatValue)) {
            return false;
          }
        }
      }
      return true;
    }
  }

  static class HTXAddressTracker extends ZooKeeperNodeTracker {

    public HTXAddressTracker(ZooKeeperWatcher watcher, String node,
        Abortable abortable) {
      super(watcher, node, abortable);
    }

    public ServerName getTMAddress() {
      byte[] bytes = getData(false);
      return bytes == null ? null : ServerName.parseVersionedServerName(bytes);
    }

  }

  static class HTXConnectionImplementation implements HTXConnection {

    private final HConnection connection;
    private final HTXAddressTracker tracker;
    private volatile TransactionManagerInterface tm;
    private volatile VersionTracker vt;

    HTXConnectionImplementation(Configuration conf) throws IOException {
      this.connection = HConnectionManager.getConnection(conf);
      ZooKeeperWatcher watcher = connection.getZooKeeperWatcher();
      // TODO make znode configurable
      this.tracker = new HTXAddressTracker(watcher, "/hbase/tm", this);
      this.tracker.start();
    }

    @Override
    public HConnection getHConnection() {
      return connection;
    }

    @Override
    public TransactionManagerInterface getTransactionManager()
        throws IOException {
      if (tm == null) {
        synchronized (this) {
          if (tm == null)
            tm = get(TransactionManagerInterface.class,
                TransactionManagerInterface.VERSION);
        }
      }
      return tm;
    }

    @Override
    public VersionTracker getVersionTracker() throws IOException {
      if (vt == null) {
        synchronized (this) {
          if (vt == null)
            vt = get(VersionTracker.class, VersionTracker.VERSION);
        }
      }
      return vt;
    }

    @SuppressWarnings("unchecked")
    private <T extends VersionedProtocol> T get(Class<T> service, long version)
        throws IOException {
      // TODO assumes TM is running
      ServerName sn = tracker.getTMAddress();
      InetSocketAddress isa = new InetSocketAddress(sn.getHostname(),
          sn.getPort());
      Configuration conf = connection.getConfiguration();
      int rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
      return (T) HBaseRPC.getProxy(service, version, isa, conf, rpcTimeout);
    }

    @Override
    public void close() throws IOException {
      if (tm != null)
        HBaseRPC.stopProxy(tm);
      if (vt != null)
        HBaseRPC.stopProxy(vt);
      connection.close();
    }

    @Override
    public void abort(String why, Throwable e) {
      // TODO
    }

    @Override
    public boolean isAborted() {
      // TODO
      return false;
    }

  }
}