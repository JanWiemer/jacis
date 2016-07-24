/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

public class JacisStaleObjectException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** detail message describing the reason for the stale object exception */
  private String details;

  public JacisStaleObjectException(String message) {
    super(message);
  }

  public String getDetails() {
    return details;
  }

  public JacisStaleObjectException setDetails(String details) {
    this.details = details;
    return this;
  }

}
