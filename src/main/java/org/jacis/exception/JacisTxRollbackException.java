/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Exception attached to an exception thrown during commit to hold some additional information regarding the committed JACIS transaction..
 *
 * @author Jan Wiemer
 */
public class JacisTxRollbackException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTxRollbackException(String message) {
    super(message);
  }

  public JacisTxRollbackException(String message, Throwable cause) {
    super(message, cause);
  }
}
