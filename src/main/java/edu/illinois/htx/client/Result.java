package edu.illinois.htx.client;

import java.util.NavigableMap;

public class Result {

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> map;

  public Result(NavigableMap<byte[], NavigableMap<byte[], byte[]>> map) {
    this.map = map;
  }

  public byte[] getValue(byte[] family, byte[] qualifier) {
    NavigableMap<byte[], byte[]> m = map.get(family);
    return m == null ? null : m.get(qualifier);
  }
}
