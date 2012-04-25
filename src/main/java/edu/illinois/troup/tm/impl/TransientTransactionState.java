package edu.illinois.troup.tm.impl;

import edu.illinois.troup.tm.GroupTransactionState;

interface TransientTransactionState extends GroupTransactionState {
  public static final int CREATED = 7;
  public static final int BLOCKED = 8;
}