/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.dirtycheck;

import org.jacis.plugin.dirtycheck.object.JacisDirtyTrackingObject;

/**
 * Implementation of the {@link JacisDirtyCheck} that is based on objects
 * tracking their dirty state and provide this state via the {@link JacisDirtyTrackingObject#isDirty()} method.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
public class StoreEntryBasedDirtyCheck<K, TV extends JacisDirtyTrackingObject> implements JacisDirtyCheck<K, TV> {

  @Override
  public boolean isDirty(K key, TV originalValue, TV currentValue) {
    if(currentValue==null) {
      return originalValue!=null;
    } else {
      return currentValue.isDirty();
    }
  }
}
