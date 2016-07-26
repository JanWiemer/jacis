/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * = Listener that is informed on the lifecycle events of a transaction.
 *
 * Listeners implementing this interface can be registered at a JACIS container
 * by passing it to the method {@link JacisContainer#registerTransactionListener(JacisTransactionListener)}.
 * Once registered on each lifecycle event of any transaction the container has joined causes the
 * corresponding lifecycle callback method to be invoked.
 *
 * The abstract {@link JacisTransactionListenerAdapter} class can be extended to simplify implementing this interface.
 * Since this adapter class provides empty default implementations for all methods declared in this interface.
 * This way the actual implementation only has to overwrite the desired methods.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings("EmptyMethod")
public interface JacisTransactionListener {

  /**
   * Callback method called before the prepare phase of a transaction is executed for the stores of the container.
   *
   * @param container Reference to the corresponding container instance.
   * @param tx        Handle for the transaction for which the callback method is invoked.
   */
  void beforePrepare(JacisContainer container, JacisTransactionHandle tx);

  /**
   * Callback method called after the prepare phase of a transaction is executed for the stores of the container.
   * @param container Reference to the corresponding container instance.
   * @param tx Handle for the transaction for which the callback method is invoked.
   */
  void afterPrepare(JacisContainer container, JacisTransactionHandle tx);

  /**
   * Callback method called before the commit phase of a transaction is executed for the stores of the container.
   * @param container Reference to the corresponding container instance.
   * @param tx Handle for the transaction for which the callback method is invoked.
   */
  void beforeCommit(JacisContainer container, JacisTransactionHandle tx);

  /**
   * Callback method called after the commit phase of a transaction is executed for the stores of the container.
   * @param container Reference to the corresponding container instance.
   * @param tx Handle for the transaction for which the callback method is invoked.
   */
  void afterCommit(JacisContainer container, JacisTransactionHandle tx);

  /**
   * Callback method called before a rollback for a transaction is executed for the stores of the container.
   * @param container Reference to the corresponding container instance.
   * @param tx Handle for the transaction for which the callback method is invoked.
   */
  void beforeRollback(JacisContainer container, JacisTransactionHandle tx);

  /**
   * Callback method called after a rollback for a transaction is executed for the stores of the container.
   * @param container Reference to the corresponding container instance.
   * @param tx Handle for the transaction for which the callback method is invoked.
   */
  void afterRollback(JacisContainer container, JacisTransactionHandle tx);

}
