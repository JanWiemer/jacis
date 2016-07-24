/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

public class JacisNoTransactionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisNoTransactionException(String message) {
    super(message);
  }

}
