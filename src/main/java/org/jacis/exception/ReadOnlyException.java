/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Thrown when trying to modify an object in read only mode.
 *
 * @author Jan Wiemer
 */
public class ReadOnlyException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public ReadOnlyException(String s) {
    super(s);
  }
}
