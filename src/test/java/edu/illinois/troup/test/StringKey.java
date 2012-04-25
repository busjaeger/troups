package edu.illinois.troup.test;

import edu.illinois.troup.tm.Key;

public class StringKey implements Key, Comparable<StringKey> {

  private final String s;

  public StringKey(String s) {
    assert s != null : "string must not be null";
    this.s = s;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof StringKey && ((StringKey) obj).s.equals(s);
  }

  @Override
  public int hashCode() {
    return s.hashCode();
  }

  @Override
  public String toString() {
    return s;
  }

  @Override
  public int compareTo(StringKey o) {
    return s.compareTo(o.s);
  }
}
