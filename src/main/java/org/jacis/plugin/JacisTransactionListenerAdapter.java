/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

public abstract class JacisTransactionListenerAdapter implements JacisTransactionListener {

  @Override
  public void beforePrepare(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

  @Override
  public void afterPrepare(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

  @Override
  public void beforeCommit(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

  @Override
  public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

  @Override
  public void beforeRollback(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

  @Override
  public void afterRollback(JacisContainer container, JacisTransactionHandle tx) {
    // empty
  }

}
