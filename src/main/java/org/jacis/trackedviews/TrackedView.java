/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.trackedviews;

import java.util.List;

import org.jacis.plugin.objectadapter.cloning.JacisCloneable;
import org.jacis.store.JacisStoreImpl;

/**
 * A tracked view can be registered at a JACIS store to automatically keep a view (typically some accumulated values) up to date.
 * The store keeps the registered instance and tracks all committed changes at this instance.
 * Accessing a tracked view always causes the JACIS store to create a snapshot (a cloned instance) of the view maintained in the store (only reflecting committed modifications)
 * On access the modifications pending in the current TX are tracked on the snapshot.
 * Modifications done in afterwards in the same transaction as getting the tracked view are tracked as well on the snapshot.
 * 
 * <b>Caution:</b> a tracked view must not modify the objects passed to the method tracking the modifications!
 *
 * @param <V> The type of the original values (from the store)
 * @author Jan Wiemer
 */
public interface TrackedView<V> extends JacisCloneable<TrackedView<V>> {

  /**
   * Track modification of the passed object.
   * The method is called during commit for each modified object.
   * The method gets the old value (the original value at the point of time
   * the value was first cloned to the transactional view of the currently committed transaction)
   * and the new value that is now committed to the store.
   *
   * @param oldValue the value was first cloned to the transactional
   * @param newValue the new value that is now committed to the store
   */
  void trackModification(V oldValue, V newValue);

  /**
   * The implementation of this method should check the consistency of the tracked view.
   * The method is called after executing a commit is finished.
   * Note that the method is only called if this is specified in the specification of the store
   * (see {@link org.jacis.container.JacisObjectTypeSpec#checkViewsOnCommit} (default is <code>false</code>)).
   * The method gets all committed values as parameter.
   * Usually the implementation goes through all these values and calculates the expected values of
   * the tracked view on these values. If these values differ from the actually tracked values the
   * method should throw an exception.
   * 
   * @param values all committed values stored in the store (after the just finished commit)
   */
  void checkView(List<V> values);

  /**
   * Clear the tracked view.
   * After executing this method the view should reflect the state where no committed values are stored.
   * The method is only called by the {@link JacisStoreImpl#clear()} method.
   */
  void clear();

  /**
   * @return if the implementation of the view is thread safe. Default is <code>false</code>. Overwrite this method to declare a view to be thread safe.
   */
  default boolean isThreadSafe() {
    return false;
  }

}
