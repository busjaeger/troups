package edu.illinois.troups.tm.region.log;

import java.io.EOFException;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.MD5Hash;

import edu.illinois.troups.tm.region.HKey;

/**
 * Currently implementation does not perform well.
 */
public class HSequenceFileLogStore implements GroupLogStore {

  private final FileSystem fs;
  private final Path groupsDir;
  private final ConcurrentMap<HKey, Path> files;

  public HSequenceFileLogStore(FileSystem fs, Path groupsDir) {
    this.fs = fs;
    this.groupsDir = groupsDir;
    this.files = new ConcurrentHashMap<HKey, Path>();
  }

  @Override
  public NavigableMap<Long, byte[]> open(HKey groupKey) throws IOException {
    NavigableMap<Long, byte[]> records = new TreeMap<Long, byte[]>();

    String path = getPath(groupKey);
    Path groupPath = new Path(groupsDir, path);
    if (fs.exists(groupPath)) {
      FSDataInputStream is = fs.open(groupPath);
      try {
        while (true) {
          long sid = is.readLong();
          int len = is.readInt();
          byte[] b = new byte[len];
          is.readFully(b);
          records.put(sid, b);
        }
      } catch (EOFException e) {
        // ignore
      } finally {
        is.close();
      }
    } else {
      fs.createNewFile(groupPath);
    }
    files.put(groupKey, groupPath);
    return records;
  }

  @Override
  public void append(HKey groupKey, long sid, byte[] value) throws IOException {
    Path groupPath = files.get(groupKey);
    if (groupPath == null)
      throw new IllegalStateException("Group not open");
    synchronized (groupPath) {
      FSDataOutputStream out = fs.append(groupPath);
      try {
        out.writeLong(sid);
        out.writeInt(value.length);
        out.write(value);
      } finally {
        out.close();
      }
    }
  }

  @Override
  public void truncate(HKey groupKey, long sid) throws IOException {
    // TODO figure out good way to do this
  }

  // TODO not ideal
  String getPath(HKey groupKey) {
    return MD5Hash.getMD5AsHex(groupKey.getRow());
  }

}
