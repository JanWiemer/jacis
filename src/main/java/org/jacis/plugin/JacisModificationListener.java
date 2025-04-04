/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.JacisApi;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisModificationVetoException;
import org.jacis.store.JacisStoreImpl;

/**
 * Listener that gets notified on each modification during commit.
 * <p>
 * A listener implementing this interface can be registered at a transactional store
 * by passing it to the method {@link JacisStoreImpl#registerModificationListener(JacisModificationListener)}.
 * Once registered the method {@link #onModification(Object, Object, Object, JacisTransactionHandle)} of the listener is called
 * for each modification on the committed values in the store. The callback method is called during the commit phase
 * of the transaction when the modified values in the transactional view are written back to the store.
 * Note that a modification listener can only be registered for a store if this is configured to track the
 * original values of a transaction (see {@link JacisObjectTypeSpec#isTrackOriginalValueEnabled()}).
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisModificationListener<K, V> {

  /**
   * Callback method called before the prepare phase of a transaction for value modified during the transaction.
   * Implementations of this method may do further modifications on the passes value to commit, based on the tracked modifications.
   * This is done before the prepare phase of the transaction is actually started for the store.
   *
   * @param key           The key of the modified object
   * @param oldValue      The original value of the modified object when it was copied to the transactional view (read only).
   * @param valueToCommit The new modified value that was updated during the transaction and may be adjusted in this method (writable).
   * @param tx            The transaction that is currently committed.
   */
  default void onAdjustBeforePrepare(K key, V oldValue, V valueToCommit, JacisTransactionHandle tx) {
    // default implementation empty
  }

  /**
   * Callback method called during the prepare phase of a transaction for each modified value.
   * Note that implementing methods can throw an {@link JacisModificationVetoException} to deny the modification and rollback the transaction.
   *
   * @param key      The key of the modified object
   * @param oldValue The original value of the modified object when it was copied to the transactional view (read only).
   * @param newValue The new modified value that is written back to the committed values (read only).
   * @param tx       The transaction that is currently committed.
   * @throws JacisModificationVetoException to deny the modification and rollback the transaction
   */
  default void onPrepareModification(K key, V oldValue, V newValue, JacisTransactionHandle tx) throws JacisModificationVetoException {
    // default implementation empty
  }

  /**
   * Callback method called during the commit phase of a transaction for each modified value written back
   * from the transactional view to the store of committed values.
   * Note that implementing methods should not throw an exception since the original transaction could be broken by this.
   *
   * @param key      The key of the modified object
   * @param oldValue The original value of the modified object when it was copied to the transactional view (read only).
   * @param newValue The new modified value that is written back to the committed values (read only).
   * @param tx       The transaction that is currently committed.
   */
  void onModification(K key, V oldValue, V newValue, JacisTransactionHandle tx);

  /**
   * @return if the implementation of the modification listener is thread safe. Default is <code>false</code>. Overwrite this method to declare a view to be thread safe.
   */
  default boolean isThreadSafe() {
    return false;
  }
}
