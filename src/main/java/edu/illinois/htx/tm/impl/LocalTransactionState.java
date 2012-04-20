package edu.illinois.htx.tm.impl;

import edu.illinois.htx.tm.TransactionState;

interface LocalTransactionState<K> extends TransactionState {
  public static final int CREATED = 5;
  public static final int BLOCKED = 6;
}