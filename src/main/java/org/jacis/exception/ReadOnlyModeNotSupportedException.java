/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Thrown when trying to access an object in read only mode when the object adapter
 * does not support a read only mode for this object
 * and is configured to throw an exception in this case.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class ReadOnlyModeNotSupportedException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public ReadOnlyModeNotSupportedException(String s) {
    super(s);
  }
}
