/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Exception thrown on the attempt to update an object
 * while the current transaction is already prepared for commit.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTransactionAlreadyPreparedForCommitException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTransactionAlreadyPreparedForCommitException(String message) {
    super(message);
  }

}
