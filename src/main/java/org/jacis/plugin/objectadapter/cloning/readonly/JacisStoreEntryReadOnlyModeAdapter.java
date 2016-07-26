/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning.readonly;

/**
 * @author Jan Wiemer
 *
 * The read only mode adapter is used to define if and how objects are switched to read only mode and back.
 * 
 * @param <V> The type of the values that have to be cloned.
 */
public interface JacisStoreEntryReadOnlyModeAdapter<V> {

  V switchToReadOnlyMode(V value);

  V switchToReadWriteMode(V value);

  boolean isReadOnly(V value);

}
