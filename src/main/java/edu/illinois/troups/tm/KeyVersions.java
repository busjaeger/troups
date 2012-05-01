package edu.illinois.troups.tm;

import com.google.common.base.Function;

public interface KeyVersions<K extends Key> {

  public static Function<KeyVersions<Key>, Key> getKey = new Function<KeyVersions<Key>, Key>() {
    @Override
    public Key apply(KeyVersions<Key> input) {
      return input.getKey();
    }
  };

  K getKey();

  Iterable<Long> getVersions();

}
