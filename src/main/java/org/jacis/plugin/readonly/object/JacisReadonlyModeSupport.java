/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.readonly.object;

import org.jacis.JacisApi;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * This interface can be implemented by objects that support switching between read-write and read-only mode.
 * The default implementation (class {@link DefaultJacisStoreEntryReadOnlyModeAdapter})
 * for the {@link JacisStoreEntryReadOnlyModeAdapter} interface uses this interface to switch the read / write mode.
 * <p>
 * Note that the implementation of the object is responsible to ensure that an object in read-only mode really prevents
 * all modifications, usually by throwing an exception (e.g. an {@link org.jacis.exception.ReadOnlyException}).
 * <p>
 * An abstract base class simplifying to implement this interface is the {@link AbstractReadOnlyModeSupportingObject} class.
 *
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisReadonlyModeSupport {

  /**
   * Switches the object into the read-only mode
   */
  void switchToReadOnlyMode();

  /**
   * Switches the object into the read-write mode
   */
  void switchToReadWriteMode();

  /**
   * @return if the object is currently in the read-only mode
   */
  boolean isReadOnly();

  /**
   * @return if the object is writable for the current thread
   */
  boolean isWritable();

  /**
   * Switches the read / write mode depending on the passed parameter
   *
   * @param readOnlyMode if 'true' switch to read-only mode, if 'false' switch to read-write mode
   */
  default void switchToReadOnlyMode(boolean readOnlyMode) {
    if (readOnlyMode) {
      switchToReadOnlyMode();
    } else {
      switchToReadWriteMode();
    }
  }

}
