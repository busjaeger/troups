package edu.illinois.troups.tm.impl;

import edu.illinois.troups.tm.GroupTransactionState;

interface TransientTransactionState extends GroupTransactionState {
  public static final int CREATED = 7;
  public static final int BLOCKED = 8;
}