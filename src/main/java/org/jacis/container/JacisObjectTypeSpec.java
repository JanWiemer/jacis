package org.jacis.container;

import org.jacis.plugin.cloning.DefaultJacisStoreEntryCloneAdapter;
import org.jacis.plugin.cloning.JacisStoreEntryCloneAdapter;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * @author Jan Wiemer
 * 
 * Specification for an object type that shall be stored.
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public class JacisObjectTypeSpec<K, V> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private boolean trackOriginalValue;
  private boolean checkViewsOnCommit;
  private JacisStoreEntryCloneAdapter<V> cloneAdapter = new DefaultJacisStoreEntryCloneAdapter<>();
  private JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter = new DefaultJacisStoreEntryReadOnlyModeAdapter<>();

  public JacisObjectTypeSpec(Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    trackOriginalValue = true;
    checkViewsOnCommit = false;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  public JacisStoreEntryCloneAdapter<V> getCloneAdapter() {
    return cloneAdapter;
  }

  public JacisStoreEntryReadOnlyModeAdapter<V> getReadOnlyModeAdapter() {
    return readOnlyModeAdapter;
  }

  public boolean isTrackOriginalValueEnabled() {
    return trackOriginalValue;
  }

  public boolean isCheckViewsOnCommit() {
    return checkViewsOnCommit;
  }

  public JacisObjectTypeSpec<K, V> setTrackOriginalValue(boolean trackOriginalValue) {
    this.trackOriginalValue = trackOriginalValue;
    return this;
  }

  public JacisObjectTypeSpec<K, V> setCheckViewsOnCommit(boolean checkViewsOnCommit) {
    this.checkViewsOnCommit = checkViewsOnCommit;
    return this;
  }

  public JacisObjectTypeSpec<K, V> setCloneAdapter(JacisStoreEntryCloneAdapter<V> cloneAdapter) {
    this.cloneAdapter = cloneAdapter;
    return this;
  }

  public JacisObjectTypeSpec<K, V> setReadOnlyModeAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter) {
    this.readOnlyModeAdapter = readOnlyModeAdapter;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(keyClass=" + keyClass.getSimpleName() + ", valueClass=" + valueClass.getSimpleName() + ")";
  }

}
