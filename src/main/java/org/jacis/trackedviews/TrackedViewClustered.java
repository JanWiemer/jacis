/*
 * Copyright (c) 2014 SSI Schaefer Noell GmbH
 */

package org.jacis.trackedviews;

import java.util.Collection;

/**
 * A clustered tracked view provides access to sub views by a key.
 * The advantage is that only the desired sub view is cloned when accessing it.
 * Caution: a tracked view must not modify the objects passed to the method tracking the modifications!
 *
 * @param <V> The type of the original values / objects from the store
 * @param <SVK> The key type for the sub views
 * @param <SVC> The type of the sub views
 */
public interface TrackedViewClustered<V, SVK, SVT extends TrackedView<V>> extends TrackedView<V> {

  public SVT getSubView(SVK key);

  public Collection<SVK> getSubViewKeys();

}
