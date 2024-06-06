/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

import java.util.Collection;

/**
 * Transaction adapter that can be registered to bind the Jacis Store to externally managed transactions.
 * <p>
 * This interface declares the methods required by the Jacis Store to interact with externally managed transactions.
 * Basically the Jacis store
 *
 * <ul>
 * <li>has to check if there is currently a transaction active,</li>
 * <li>let the container join the currently active transaction,</li>
 * <li>remove the association between the container and the external transaction (when the transaction is finished).</li>
 * </ul>
 *
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisTransactionAdapter {

  /**
   * @return If there is currently an active external transaction.
   */
  boolean isTransactionActive();

  /**
   * Let the container (and therefore all its stores) join the currently active external transaction.
   * Inside the store the transaction is represented by the returned transaction handle.
   * If there is no external transaction the method returns <code>null</code>.
   *
   * @param container The container (type {@link JacisContainer}) that shall join the external transaction
   * @return The transaction handle (type {@link JacisTransactionHandle}) representing the external transaction inside the jacis store.
   */
  JacisTransactionHandle joinCurrentTransaction(JacisContainer container);

  /**
   * Remove the association between the container and the external transaction (when the transaction is finished).
   */
  @SuppressWarnings("SpellCheckingInspection")
  void disjoinCurrentTransaction(JacisTransactionHandle transaction);

  /**
   * @param externalTransaction The external (global or local transaction)
   * @return the JACIS transaction handle associated with the external transaction
   */
  JacisTransactionHandle getTransactionHandle(Object externalTransaction);

  /**
   * @return all JACIS transaction handles for all active transactions
   */
  Collection<JacisTransactionHandle> getAllTransactionHandles();

}
