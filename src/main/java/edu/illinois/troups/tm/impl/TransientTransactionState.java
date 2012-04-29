package edu.illinois.troups.tm.impl;

import edu.illinois.troups.tm.TransactionState;

interface TransientTransactionState extends TransactionState {
  public static final int CREATED = 7;
  public static final int BLOCKED = 8;
}