package edu.illinois.htx.tm.impl;

interface XATransactionState<K> extends LocalTransactionState<K> {
  public static final int JOINED = 6;
  public static final int PREPARED = 7;
}