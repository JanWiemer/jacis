/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.container;

import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.plugin.dirtycheck.JacisDirtyCheck;
import org.jacis.plugin.dirtycheck.StoreEntryBasedDirtyCheck;
import org.jacis.plugin.dirtycheck.object.JacisDirtyTrackingObject;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * = Specification of the Objects in a Store
 *
 * Specification of an object type that shall be stored in a transational store.
 * The pain purpose of the specification is to define the types (classes) for the keys and the values.
 *
 * Furthermore some configuration is defined in this class how the object store should deal with objects of this type.
 * The basic concept how the transactional store works is that it stores an internal representation
 * of the committed versions of all objects. This representation is not visible to the outside.
 * Each read access from the outside gets back a copy (clone) of the internal representation
 * only visible for the current transaction. If an object should be updated inside a transaction the external
 * representation has to be modified and the store explicitly has to be notified about the change
 * by calling an update method (there is no automatic) dirty checking.
 *
 * The mechanism how the objects are copied from the internal representation and back can be customized
 * (e.g. by cloning or by serialization and de-serialization).
 * The object specification stores an {@link #objectAdapter} (type {@link JacisObjectAdapter}) implementing this mechanism.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
@SuppressWarnings("unused")
public class JacisObjectTypeSpec<K, TV, CV> {

  /** Type of the keys in the store */
  private final Class<K> keyClass;
  /** Type of the values in the store */
  private final Class<TV> valueClass;
  /** The object adapter defining how to copy the committed values to the transactional view and back. */
  private JacisObjectAdapter<TV, CV> objectAdapter;
  /** The dirty check used to automatically set an transactional view to updated if it has changed */
  private JacisDirtyCheck<K, TV> dirtyCheck;
  /** Defining if the store keeps track of the original value of an object at the time it was copied to the transactional view (default: 'false') */
  private boolean trackOriginalValue = false;
  /** Defines if all registered tracked views are checked for consistency on each internalCommit (default: 'false'). */
  private boolean checkViewsOnCommit = false;

  public JacisObjectTypeSpec(Class<K> keyClass, Class<TV> valueClass, JacisObjectAdapter<TV, CV> objectAdapter) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.objectAdapter = objectAdapter;
    trackOriginalValue = true;
    checkViewsOnCommit = false;
  }

  /** @return The type of the keys in the store */
  public Class<K> getKeyClass() {
    return keyClass;
  }

  /** @return The type of the values in the store */
  public Class<TV> getValueClass() {
    return valueClass;
  }

  /** @return The object adapter defining how to copy the committed values to the transactional view and back. */
  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  /**
   * Set the passed object adapter defining how to copy the committed values to the transactional view and back.
   *
   * @param objectAdapter the object adapter to set.
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setObjectAdapter(JacisObjectAdapter<TV, CV> objectAdapter) {
    this.objectAdapter = objectAdapter;
    return this;
  }

  /** @return The dirty check used to automatically set an transactional view to updated if it has changed. */
  public JacisDirtyCheck<K, TV> getDirtyCheck() {
    return dirtyCheck;
  }

  /**
   * Set the passed dirty check defining how to copy the committed values to the transactional view and back.
   *
   * @param dirtyCheck the dirty check to set.
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setDirtyCheck(JacisDirtyCheck<K, TV> dirtyCheck) {
    this.dirtyCheck = dirtyCheck;
    return this;
  }

  /** @return if the store keeps track of the original value of an object at the time it was copied to the transactional view (default: 'false'). */
  public boolean isTrackOriginalValueEnabled() {
    return trackOriginalValue;
  }

  /** @return if all registered tracked views are checked for consistency on each internalCommit (default: 'false'). */
  public boolean isCheckViewsOnCommit() {
    return checkViewsOnCommit;
  }

  /**
   * Sets if all registered tracked views are checked for consistency on each internalCommit (default: 'false').
   * Note that the value should only be set before the corresponding store is used, otherwise the behavior is undefined.
   * @param checkViewsOnCommit Defining if all registered tracked views are checked for consistency on each internalCommit.
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setCheckViewsOnCommit(boolean checkViewsOnCommit) {
    this.checkViewsOnCommit = checkViewsOnCommit;
    return this;
  }

  /**
   * Sets the {@link StoreEntryBasedDirtyCheck} for the store.
   *
   * @return The object type specification itself for method chaining.
   */
  @SuppressWarnings("unchecked")
  public JacisObjectTypeSpec<K, TV, CV> setObjectBasedDirtyCheck() {
    if (!(JacisDirtyTrackingObject.class.isAssignableFrom(valueClass))) {
      throw new IllegalStateException("Object based dirty check only suitable for object value types implementing " + JacisDirtyTrackingObject.class);
    }
    this.dirtyCheck = new StoreEntryBasedDirtyCheck();
    return this;
  }

  /**
   * Sets if the store should keep track of the original value of an object at the time it was copied to the transactional view (default: 'false').
   * Note that the value should only be set before the corresponding store is used, otherwise the behavior is undefined.
   * @param trackOriginalValue Defining if the store keeps track of the original value of an object at the time it was copied to the transactional view.
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setTrackOriginalValue(boolean trackOriginalValue) {
    this.trackOriginalValue = trackOriginalValue;
    return this;
  }

  /** @return The store identifier (containing key and value class) for this object type specification. */
  public StoreIdentifier getStoreIdentifier() {
    return new StoreIdentifier(keyClass, valueClass);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valueClass.getSimpleName() + ")";
  }

}
