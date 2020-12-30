/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception thrown in case of internal problems dealing with an external transaction system.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTransactionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTransactionException(Throwable cause) {
    super(cause);
  }
}
