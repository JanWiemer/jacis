package org.jacis.plugin.objectadapter.readonly;

/**
 * @author Jan Wiemer
 *
 * The read only mode adapter is used to define if and how objects are switched to read only mode and back.
 * 
 * @param <V> The type of the values that have to be cloned.
 */
public interface JacisStoreEntryReadOnlyModeAdapter<V> {

  public V switchToReadOnlyMode(V value);

  public V switchToReadWriteMode(V value);

  public boolean isReadOnly(V value);

}
