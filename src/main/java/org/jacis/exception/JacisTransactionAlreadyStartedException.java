/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception thrown on the attempt to start a new (local) transaction
 * while there is already a transaction active.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTransactionAlreadyStartedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTransactionAlreadyStartedException(String message) {
    super(message);
  }

}
