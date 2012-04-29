package edu.illinois.troups.tmg.impl;

import java.io.IOException;
import java.util.NavigableMap;

public interface GroupLogStore {

  NavigableMap<Long, byte[]> open(HKey groupKey) throws IOException;

  void append(HKey groupKey, long sid, byte[] value) throws IOException;

  void truncate(HKey groupKey, long sid) throws IOException;

//  Iterable<byte[]> getGroupKeys();

}