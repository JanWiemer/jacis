/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * Adapter class to simplify creating an implementation of the {@link JacisTransactionListener} interface.
 * A class implementing this interface can extend this class providing empty default implementation for
 * all methods declared in the interface. So the implementation only has to overwrite the desired method and skip the other ones.
 *
 * @author Jan Wiemer
 */
@JacisApi
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

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
