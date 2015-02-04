package org.jacis.trackedviews;

import java.util.List;

import org.jacis.plugin.objectadapter.cloning.JacisCloneable;

/**
 * A tracked view can be registered at a JACIS store to automatically keep a view (typically some cummulated values) up to date.
 * The store keeps the registered instance and tracks all committed changes at this instance.
 * On access the modifications pending in the current TX are tracked as well (on a cloned instance of the view).
 * Caution: a tracked view must not modify the objects passed to the method tracking the modifications!
 *
 * @param <V> The type of the original values (from the store)
 */
public interface TrackedView<V> extends JacisCloneable<TrackedView<V>> {

  public void trackModification(V oldValue, V newValue);

  public void checkView(List<V> values);

  public void clear();

}
