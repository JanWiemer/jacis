/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Exception thrown if an internal Jacis Error happenes.
 *
 * @author Jan Wiemer
 */
public class JacisInternalException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisInternalException(String message) {
    super(message);
  }

}
