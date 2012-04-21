package edu.illinois.htx.tm.impl;

import edu.illinois.htx.tm.TransactionState;

interface TransientTransactionState extends TransactionState {
  public static final int CREATED = 7;
  public static final int BLOCKED = 8;
}