/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Thrown when trying to access an object in read only mode when the object adapter
 * does not support a read only mode for this object
 * and is configured to throw an exception in this case.
 *
 * @author Jan Wiemer
 */
public class ReadOnlyModeNotSupportedException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public ReadOnlyModeNotSupportedException(String s) {
    super(s);
  }
}
