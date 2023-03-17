/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.extension.objectadapter.cloning.microstream;

import java.io.Serializable;

import org.jacis.plugin.objectadapter.cloning.AbstractJacisCloningObjectAdapter;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * Generic implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter} copying the objects
 * to and from the transactional view by means of Microstream serialization.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public class JacisMicrostreamCloningObjectAdapter<V extends Serializable> extends AbstractJacisCloningObjectAdapter<V> {

  private final MicrostreamObjectCopier copier = MicrostreamObjectCopier.New();

  /**
   * Create a cloning object adapter with the passed read only mode adapter.
   * 
   * @param readOnlyModeAdapters Adapter to switch an object between read-only and read-write mode (if supported).
   */
  public JacisMicrostreamCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    super(readOnlyModeAdapters);
  }

  /**
   * Create a cloning object adapter with a default read only mode adapter (see {@link DefaultJacisStoreEntryReadOnlyModeAdapter}).
   */
  public JacisMicrostreamCloningObjectAdapter() {
    super();
  }

  @Override
  protected V cloneValue(V value) {
    return copier.copy(value);
  }

}
