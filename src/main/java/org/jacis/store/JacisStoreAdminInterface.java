/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * Administration interface for a JACIS store.
 *
 * This interface provides methods to add or remove transaction listeners or maintain the registered tracked views.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public interface JacisStoreAdminInterface<K, TV, CV> {

  /** @return the store */
  JacisStore<K, TV> getStore();

  /** @return the store identifier uniquely identifying this store inside the container */
  JacisContainer.StoreIdentifier getStoreIdentifier();

  /** @return the object type specification for the objects stored in this store */
  JacisObjectTypeSpec<K, TV, CV> getObjectTypeSpec();

  /** @return the object adapter defining how to copy objects from the committed view to a transactional view and back */
  JacisObjectAdapter<TV, CV> getObjectAdapter();

  /**
   * Returns a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object
   * (regarding the committed values and the current transactional view).
   *
   * @param key The key of the desired object.
   * @return a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object.
   */
  StoreEntryInfo<K, TV> getObjectInfo(K key);
}
