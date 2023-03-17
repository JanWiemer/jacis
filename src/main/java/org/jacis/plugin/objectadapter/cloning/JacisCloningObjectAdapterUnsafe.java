/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.JacisApi;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * Implementation of the {@link AbstractJacisCloningObjectAdapter} cloning the objects based on the Java clone method.
 * The behavior is similar to the {@link JacisCloningObjectAdapter} but returning a read only version of the object does not first check if the object supports a read only mode.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
@JacisApi
@SuppressWarnings({"unused", "UnusedReturnValue"}) // since this is an API of the library
public class JacisCloningObjectAdapterUnsafe<V> extends JacisCloningObjectAdapter<V> {

  /**
   * Create a cloning object adapter with the passed read only mode adapter.
   * 
   * @param readOnlyModeAdapters Adapter to switch an object between read-only and read-write mode (if supported).
   */
  public JacisCloningObjectAdapterUnsafe(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    super(readOnlyModeAdapters);
    if (readOnlyModeAdapters == null) {
      throw new IllegalArgumentException(this.getClass().getName() + " can only be initialized with a read only mode adapter!");
    }
  }

  /**
   * Create a cloning object adapter with a default read only mode adapter (see {@link DefaultJacisStoreEntryReadOnlyModeAdapter}).
   */
  public JacisCloningObjectAdapterUnsafe() {
    super();
  }

  @Override
  public V cloneCommitted2ReadOnlyTxView(V value) {
    return value;
  }

}
