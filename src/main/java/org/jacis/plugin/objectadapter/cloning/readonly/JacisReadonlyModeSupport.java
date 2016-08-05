/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning.readonly;

/**
 * Interface objects can implement to support switching them to read only mode and back.
 * @author Jan Wiemer
 */
public interface JacisReadonlyModeSupport {

  void switchToReadOnlyMode();

  void switchToReadWriteMode();

  boolean isReadOnly();

  default void switchToReadOnlyMode(boolean readOnlyMode) {
    if (readOnlyMode) {
      switchToReadOnlyMode();
    } else {
      switchToReadWriteMode();
    }
  }

}
