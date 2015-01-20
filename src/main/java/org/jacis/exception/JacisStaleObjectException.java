package org.jacis.exception;

public class JacisStaleObjectException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JacisStaleObjectException(String message) {
    super(message);
  }

}
