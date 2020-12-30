/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception thrown if an operation is called on a store that requires to run inside a transaction,
 * but there is no transaction is active.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisNoTransactionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisNoTransactionException(String message) {
    super(message);
  }

}
