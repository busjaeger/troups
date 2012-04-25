package edu.illinois.troups.client;

import java.util.ArrayList;
import java.util.List;

public class Delete extends Mutation {

  public Delete(byte[] row) {
    super(row);
  }

  public Delete deleteColumn(byte[] family, byte[] qualifier) {
    List<KeyValue> list = familyMap.get(family);
    if (list == null) {
      list = new ArrayList<KeyValue>();
    }
    list.add(new KeyValue(this.row, family, qualifier));
    familyMap.put(family, list);
    return this;
  }
}
