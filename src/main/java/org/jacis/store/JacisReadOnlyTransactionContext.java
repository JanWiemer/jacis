/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.store;

/**
 * Representing a read only version of the context of a store within a transaction.
 * The context contains all transactional views for objects in the store belonging to this transaction.
 * The context can be used to propagate a read only view of this context to another transaction in another thread.
 * Note that it is only possible to propagate a read only view to prevent multiple threads to work in a single transaction concurrently.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({ "WeakerAccess" })
public interface JacisReadOnlyTransactionContext {

  /**
   * @return The id of the transaction this transactional context was retrieved from.
   */
  String getTxId();

  /**
   * @return The id of the transaction this transactional context should be used in read only mode.
   */
  String getReadOnlyTxId();
}
