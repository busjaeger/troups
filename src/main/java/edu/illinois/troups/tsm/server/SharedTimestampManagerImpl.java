package edu.illinois.troups.tsm.server;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import edu.illinois.troups.tsm.NoSuchTimestampException;
import edu.illinois.troups.tsm.NotOwnerException;

public class SharedTimestampManagerImpl extends TimestampManagerImpl implements
    TimestampManagerServer {

  protected static byte[] references = Bytes.toBytes("refs");

  public SharedTimestampManagerImpl(HTablePool tablePool, byte[] tableName,
      byte[] familyName, long batchSize, long tsTimeout) throws IOException {
    super(tablePool, tableName, familyName, batchSize, tsTimeout);
    loadPersisted();
    if (!timestamps.isEmpty())
      this.lastReclaimed.set(timestamps.firstKey());
  }

  @Override
  public long acquireShared() throws IOException {
    return super.acquire();
  }

  @Override
  protected Timestamp newTimestamp() {
    return new SharedTimestamp();
  }

  @Override
  public boolean releaseShared(long ts) throws IOException {
    Timestamp t = timestamps.get(ts);
    if (t == null)
      return false;
    if (!(t instanceof SharedTimestamp))
      throw new IllegalArgumentException(ts + " not a shared timestamp");
    SharedTimestamp timestamp = (SharedTimestamp) t;
    synchronized (timestamp) {
      if (timestamp.isReleased())
        return false;
      if (timestamp.isPersisted()) {
        HTableInterface table = tablePool.getTable(tableName);
        try {
          Delete delete = new Delete(Bytes.toBytes(ts), ts, null);
          delete.deleteColumn(familyName, references);
          table.delete(delete);
        } finally {
          table.close();
        }
        timestamp.setPersisted(false);
      }
      return timestamp.release();
    }
  }

  @Override
  public long acquireReference(long ts) throws NoSuchTimestampException,
      IOException {
    SharedTimestamp timestamp = getSharedTimestamp(ts);
    synchronized (timestamp) {
      if (timestamp.isPersisted())
        throw new IllegalStateException(ts + " timestamp already persisted");
      return timestamp.nextReferenceID();
    }
  }

  @Override
  public boolean releaseReference(long ts, long rid) throws IOException {
    Timestamp t = timestamps.get(ts);
    if (t == null)
      return false;
    if (!(t instanceof SharedTimestamp))
      throw new IllegalArgumentException(ts + " not a shared timestamp");
    SharedTimestamp timestamp = (SharedTimestamp) t;
    synchronized (timestamp) {
      Collection<Long> refs = timestamp.getRIDs();
      if (!refs.remove(rid))
        return false;
      if (!timestamp.isPersisted())
        return true;
      // otherwise update persistent state
      HTableInterface tsTable = tablePool.getTable(tableName);
      try {
        byte[] row = Bytes.toBytes(ts);
        Put put = new Put(row, ts);
        byte[] newValue = toByteArray(refs);
        put.add(familyName, references, newValue);
        tsTable.put(put);
      } catch (IOException e) {
        timestamp.getRIDs().add(rid);
        throw e;
      } finally {
        tsTable.close();
      }
      if (refs.isEmpty())
        release(ts);
      return true;
    }
  }

  @Override
  public boolean isReferencePersisted(long ts, long rid)
      throws NoSuchTimestampException, IOException {
    SharedTimestamp sts = getSharedTimestamp(ts);
    synchronized (sts) {
      if (!sts.isPersisted())
        return false;
      return sts.getRIDs().contains(rid);
    }
  }

  @Override
  public void persistReferences(long ts, Iterable<Long> rids)
      throws NotOwnerException, IOException {
    SharedTimestamp sts = getSharedTimestamp(ts);
    synchronized (sts) {
      if (sts.isPersisted())
        throw new IllegalStateException("timestamp already persisted");
      Set<Long> refs = sts.getRIDs();
      int size = 0;
      for (Long ref : rids) {
        if (!refs.contains(ref))
          throw new IllegalArgumentException("Reference " + ref
              + " has not been acquired");
        size++;
      }
      if (size != refs.size())
        throw new IllegalArgumentException(
            "Additional references have been acquired");
      HTableInterface tsTable = tablePool.getTable(tableName);
      try {
        byte[] row = Bytes.toBytes(ts);
        Put put = new Put(row, ts);
        byte[] value = toByteArray(rids);
        put.add(familyName, references, value);
        tsTable.put(put);
      } finally {
        tsTable.close();
      }
      sts.setPersisted(true);
    }
  }

  SharedTimestamp getSharedTimestamp(long ts) throws NoSuchTimestampException,
      IOException {
    Timestamp timestamp = timestamps.get(ts);
    if (timestamp == null)
      throw new NoSuchTimestampException();
    if (!(timestamp instanceof SharedTimestamp))
      throw new IOException("not a shared timestamp");
    return (SharedTimestamp) timestamp;
  }

  private byte[] toByteArray(Iterable<Long> rids) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    for (Long rid : rids)
      out.writeLong(rid);
    out.close();
    return out.getData();
  }

  private Collection<Long> fromByteArray(byte[] refs) throws IOException {
    Collection<Long> col = new ArrayList<Long>();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(refs, refs.length);
    while (true) {
      try {
        col.add(in.readLong());
      } catch (EOFException e) {
        break;
      }
    }
    in.close();
    return col;
  }

  private void loadPersisted() throws IOException {
    HTableInterface tsTable = tablePool.getTable(tableName);
    try {
      Scan scan = new Scan();
      ResultScanner scanner;
      scanner = tsTable.getScanner(scan);
      try {
        for (Result result : scanner) {
          byte[] row = result.getRow();
          if (Bytes.equals(row, counterRow))
            continue;
          byte[] refs = result.getValue(familyName, references);
          if (refs == null)
            continue;// illegal state
          Collection<Long> rids = fromByteArray(refs);
          SharedTimestamp sts = new SharedTimestamp(rids);
          timestamps.put(Bytes.toLong(row), sts);
        }
      } finally {
        scanner.close();
      }
    } finally {
      tsTable.close();
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return TimestampManagerServer.VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(getProtocolVersion(protocol, clientVersion),
        null);
  }
}
