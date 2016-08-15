/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.dirtycheck.object;

/**
 * Interface for an object automatically tracking if it is dirty.
 */
public interface JacisDirtyTrackingObject {

  /** @return if the object is dirty */
  boolean isDirty();
}
