/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.immutable;

import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * Implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter} that can be used if the store only contains
 * immutable objects. Note that this requires that the objects are never changed once they are stored in the store.
 * 
 * When using this adapter it is recommended to programmatically ensure that the objects are immutable, e.g. by making all properties final.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public class JacisImmutableObjectAdapter<V> implements JacisObjectAdapter<V, V> {

  @Override
  public V cloneCommitted2WritableTxView(V value) {
    return value;
  }

  @Override
  public V cloneTxView2Committed(V value) {
    return value;
  }

  @Override
  public V cloneCommitted2ReadOnlyTxView(V value) {
    return value;
  }

  @Override
  public V cloneTxView2ReadOnlyTxView(V value) {
    return value;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
