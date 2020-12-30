/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.plugin.persistence;

import org.jacis.JacisApi;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.store.JacisStore;

/**
 * Interface for persistence adapters that may be plugged into a JACIS store to make it a <em>durable</em> store.
 * 
 * The interface extends the {@link JacisModificationListener} interface to enable the implementation to
 * keep track of all changes to the objects in the store.
 * Furthermore the persistence adapter is notified when the transaction demarcation methods have been executed for a store.
 * Another method is called on startup to initialize the store with the data already stored persistently.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 * 
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisPersistenceAdapter<K, V> extends JacisModificationListener<K, V> {

  /**
   * Called on startup to initialize the passed JACIS store with the already persistently stored data.
   * 
   * @param store The JACIS store to initialize
   */
  void initializeStore(JacisStore<K, V> store);

  /**
   * Called after the prepare phase of the transaction has been executed for the store.
   * 
   * The persistence adapter may check if the storing during commit will probably succeed without exceptions.
   * The goal is that all exceptions should be thrown in the prepare phase and the commit phase succeeds without exceptions.
   */
  void prepareCommit();

  /**
   * Called after the commit phase of the transaction has been executed for the store.
   * 
   * During the commit phase all modifications on the objects in the store are tracked at the
   * persistence adapter using the {@link JacisModificationListener#onModification(Object, Object, Object, org.jacis.container.JacisTransactionHandle)} method.
   * The persistence adapter may collect the modifications and flush them together to the persistent store after commit in this method.
   */
  void commit();

  /**
   * Called after the rollback phase of the transaction has been executed for the store.
   * 
   * If the transaction is rolled back the persistence adapter has to forget all modifications tracked during the transaction.
   */
  void rollback();

}
