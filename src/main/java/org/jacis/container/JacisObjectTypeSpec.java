package org.jacis.container;

import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * <h2>Specification of the Objects in a Store </h2>
 * 
 * <p>
 * Specification of an object type that shall be stored in a transational store.
 * The pain purpose of the specification is to define the types (classes) for the keys and the values.
 * </p>
 * 
 * <p>
 * Furthermore some configuration is defined in this class how the object store should deal with objects of this type.
 * The basic concept how the transactional store works is that it stores an internal representation 
 * of the committed versions of all objects. This representation is not visible to the outside.
 * Each read access from the outside gets back a copy (clone) of the internal representation 
 * only visible for the current transaction. If an object should be updated inside a transaction the external
 * representation has to be modified and the store explicitly has to be notified about the change 
 * by calling an update method (there is no automatic) dirty checking.
 * </p>
 * 
 * <p>
 * The mechanism how the objects are copied from the internal representation and back can be customized 
 * (e.g. by cloning or by serialization and de-serialization).
 * The object specification stores an {@link #objectAdapter} (type {@link JacisObjectAdapter}) implementing this mechanism.
 * </p>
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public class JacisObjectTypeSpec<K, TV, CV> {

  /** Type of the keys in the store */
  private final Class<K> keyClass;
  /** Type of the values in the store */
  private final Class<TV> valueClass;
  /** The object adapter defining how to copy the committed values to the transactional view and back. */
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  /** Defining if the store keeps track of the original value of an object at the time it was copied to the transactional view. */
  private boolean trackOriginalValue;
  /** Defines if all registered tracked views are checked for consistency on each commit. */
  private boolean checkViewsOnCommit;

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

  /** Return The type of the values in the store */
  public Class<TV> getValueClass() {
    return valueClass;
  }

  /** @return The object adapter defining how to copy the committed values to the transactional view and back. */
  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  /** @return if the store keeps track of the original value of an object at the time it was copied to the transactional view. */
  public boolean isTrackOriginalValueEnabled() {
    return trackOriginalValue;
  }

  /** @return if all registered tracked views are checked for consistency on each commit. */
  public boolean isCheckViewsOnCommit() {
    return checkViewsOnCommit;
  }

  /**
   * Sets if the store should keep track of the original value of an object at the time it was copied to the transactional view.
   * Note that the value should only be set before the corresponding store is used, otherwise the behavior is undefined.
   * @param trackOriginalValue Defining if the store keeps track of the original value of an object at the time it was copied to the transactional view. 
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setTrackOriginalValue(boolean trackOriginalValue) {
    this.trackOriginalValue = trackOriginalValue;
    return this;
  }

  /**
   * Sets if all registered tracked views are checked for consistency on each commit. 
   * Note that the value should only be set before the corresponding store is used, otherwise the behavior is undefined.
   * @param checkViewsOnCommit Defining if all registered tracked views are checked for consistency on each commit. 
   * @return The object type specification itself for method chaining.
   */
  public JacisObjectTypeSpec<K, TV, CV> setCheckViewsOnCommit(boolean checkViewsOnCommit) {
    this.checkViewsOnCommit = checkViewsOnCommit;
    return this;
  }

  /** @return The store identifier (containing key and value class) for this object type specification. */
  public StoreIdentifier asStoreIdentifier() {
    return new StoreIdentifier(keyClass, valueClass);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valueClass.getSimpleName() + ")";
  }

}
