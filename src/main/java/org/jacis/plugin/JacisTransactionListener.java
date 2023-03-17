/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * Listener that is informed on the lifecycle events of a transaction.
 * <p>
 * Listeners implementing this interface can be registered at a JACIS container
 * by passing it to the method {@link JacisContainer#registerTransactionListener(JacisTransactionListener)}.
 * Once registered on each lifecycle event of any transaction the container has joined causes the
 * corresponding lifecycle callback method to be invoked.
 *
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisTransactionListener {

  /**
   * @return if this transaction listener has to be executed synchronized together with the prepare / commit / rollback.
   */
  default boolean isSynchronizedExcecutionRequired() {
    return true;
  }

  /**
   * Callback method called before the prepare phase of a transaction is executed for the stores of the container.
   *
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void beforePrepare(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

  /**
   * Callback method called after the prepare phase of a transaction is executed for the stores of the container.
   * 
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void afterPrepare(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

  /**
   * Callback method called before the commit phase of a transaction is executed for the stores of the container.
   * 
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void beforeCommit(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

  /**
   * Callback method called after the commit phase of a transaction is executed for the stores of the container.
   * 
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

  /**
   * Callback method called before a rollback for a transaction is executed for the stores of the container.
   * 
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void beforeRollback(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

  /**
   * Callback method called after a rollback for a transaction is executed for the stores of the container.
   * 
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  default void afterRollback(JacisContainer container, JacisTransactionHandle tx) {
    // default: do nothing
  }

}
