/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception thrown if an internal Jacis Error happens.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisInternalException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisInternalException(String message) {
    super(message);
  }

}
