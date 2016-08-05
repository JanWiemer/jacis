/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Thrown when trying to get a read only view of an object not supporting a read only mode..
 *
 * @author Jan Wiemer
 */
public class ReadOnlyModeNotSupportedException extends IllegalStateException {

  public ReadOnlyModeNotSupportedException(String s) {
    super(s);
  }
}
