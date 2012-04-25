package edu.illinois.troup.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Like HBase's Put, but restricted to operations troup supports, i.e. anything
 * that doesn't involve timestamps.
 */
public class Put extends Mutation {

  public Put(byte[] row) {
    super(row);
  }

  public Put add(byte[] family, byte[] qualifier, byte[] value) {
    List<KeyValue> kvs = familyMap.get(family);
    if (kvs == null)
      familyMap.put(family, kvs = new ArrayList<KeyValue>());
    kvs.add(new KeyValue(row, family, qualifier, value));
    return this;
  }

}
