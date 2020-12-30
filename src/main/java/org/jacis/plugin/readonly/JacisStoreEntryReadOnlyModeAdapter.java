/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.readonly;

import org.jacis.JacisApi;

/**
 * The read only mode adapter is used to define how objects are switched between the read-write and read-only mode (if supported).
 *
 * The default implementation (class {@link DefaultJacisStoreEntryReadOnlyModeAdapter}) is applicable for objects
 * implementing the {@link org.jacis.plugin.readonly.object.JacisReadonlyModeSupport} interface
 * and uses the methods declared in this interface for switching the mode.
 *
 * @param <V> The type of the values that should be switched between read-write and read-only mode.
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisStoreEntryReadOnlyModeAdapter<V> {

  /**
   * Returns if this adapter can switch the read / write mode for the passed object
   *
   * @param value The object to check.
   * @return if this adapter can switch the read / write mode for the passed object
   */
  boolean isApplicableTo(V value);

  /**
   * Returns if the passed object is in read-only mode
   *
   * @param value The object to check.
   * @return if the passed object is in read-only mode
   */
  boolean isReadOnly(V value);

  /**
   * Switch the read / write mode for the passed object to read-only mode
   * Note that the calling methods will work with the returned value.
   * Therefore the implementation of the method may return another instance than the passed (e.g. a proxy).
   *
   * @param value The object to switch to read-only mode
   * @return the object with the switched mode.
   */
  V switchToReadOnlyMode(V value);

  /**
   * Switch the read / write mode for the passed object to read-write mode
   * Note that the calling methods will work with the returned value.
   * Therefore the implementation of the method may return another instance than the passed (e.g. a proxy).
   *
   * @param value The object to switch to read-write mode
   * @return the object with the switched mode.
   */
  V switchToReadWriteMode(V value);

}
