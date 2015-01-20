package org.jacis.exception;

public class JacisTransactionAlreadyStartedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisTransactionAlreadyStartedException(String message) {
    super(message);
  }

}
