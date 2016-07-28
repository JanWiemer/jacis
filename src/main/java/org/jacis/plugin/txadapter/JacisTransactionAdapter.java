/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * = Transaction adapter that can be registered to bind the Jacis Store to externally managed transactions.
 *
 * This interface declares the methods required by the Jacis Store to interact with externally managed transactions.
 * Basically the Jacis store
 *
 * * has to check if there is currently a transaction active,
 * * let the container join the currently active transaction,
 * * remove the association between the container and the external transaction (when the transaction is finished).
 *
 * @author Jan Wiemer
 */
public interface JacisTransactionAdapter {

  /**
   * @return If there is currently an active external transaction.
   */
  boolean isTransactionActive();

  /**
   * Let the container (and therefore all its stores) join the currently active external transaction.
   * Inside the store the transaction is represented by the returned transaction handle.
   * If there is no external transaction the method returns 'null'.
   *
   * @param container The container (type {@link JacisContainer}) that shall join the external transaction
   * @return The transaction handle (type {@link JacisTransactionHandle}) representing the external transaction inside the jacis store.
   */
  JacisTransactionHandle joinCurrentTransaction(JacisContainer container);

  /**
   * Remove the association between the container and the external transaction (when the transaction is finished).
   */
  void disjoinCurrentTransaction();

}
