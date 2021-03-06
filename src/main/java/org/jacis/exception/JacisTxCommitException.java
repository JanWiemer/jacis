/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception attached to an exception thrown during commit to hold some additional information regarding the committed JACIS transaction.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTxCommitException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTxCommitException(String message) {
    super(message);
  }

  public JacisTxCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
