/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.trackedviews;

import java.util.Collection;

import org.jacis.JacisApi;

/**
 * A clustered tracked view provides access to sub views by a key.
 * The advantage is that only the desired sub view is cloned when accessing it.
 *
 * @param <V>   The type of the original values / objects from the store
 * @param <SVK> The key type for the sub views
 * @param <SVT> The type of the sub views
 * @author Jan Wiemer
 */
@JacisApi
public interface TrackedViewClustered<V, SVK, SVT extends TrackedView<V>> extends TrackedView<V> {

  /**
   * Return the partition / cluster of the clustered tracked view for the passed key.
   * Note that the method must not return null for any valid sub view key!
   *
   * @param key The key for the desired sub view (/ partition / cluster)
   * @return the partition / cluster of the clustered tracked view for the passed key.
   */
  SVT getSubView(SVK key);

  /** @return All keys that can be used to access one partition / cluster of the clustered tracked view. */
  Collection<SVK> getSubViewKeys();

}
