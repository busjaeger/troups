package edu.illinois.htx.tm.impl;

import edu.illinois.htx.tm.GroupTransactionState;

interface TransientTransactionState extends GroupTransactionState {
  public static final int CREATED = 7;
  public static final int BLOCKED = 8;
}