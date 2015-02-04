package org.jacis.container;

import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * @author Jan Wiemer
 * 
 * Specification for an object type that shall be stored.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 */
public class JacisObjectTypeSpec<K, TV, CV> {

  private final Class<K> keyClass;
  private final Class<TV> valueClass;
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  private boolean trackOriginalValue;
  private boolean checkViewsOnCommit;

  public JacisObjectTypeSpec(Class<K> keyClass, Class<TV> valueClass, JacisObjectAdapter<TV, CV> objectAdapter) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.objectAdapter = objectAdapter;
    trackOriginalValue = true;
    checkViewsOnCommit = false;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<TV> getValueClass() {
    return valueClass;
  }

  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  public boolean isTrackOriginalValueEnabled() {
    return trackOriginalValue;
  }

  public boolean isCheckViewsOnCommit() {
    return checkViewsOnCommit;
  }

  public JacisObjectTypeSpec<K, TV, CV> setTrackOriginalValue(boolean trackOriginalValue) {
    this.trackOriginalValue = trackOriginalValue;
    return this;
  }

  public JacisObjectTypeSpec<K, TV, CV> setCheckViewsOnCommit(boolean checkViewsOnCommit) {
    this.checkViewsOnCommit = checkViewsOnCommit;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valueClass.getSimpleName() + ")";
  }

}
