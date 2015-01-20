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
  private JacisStoreEntryCloneAdapter<V> cloneAdapter = new DefaultJacisStoreEntryCloneAdapter<>();
  private JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapter = new DefaultJacisStoreEntryReadOnlyModeAdapter<>();

  public JacisObjectTypeSpec(Class<K> keyClass, Class<V> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
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
    return true;
  }

  public boolean isCheckViewsOnCommit() {
    return true;
  }

}
