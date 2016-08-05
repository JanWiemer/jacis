/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

/**
 * Exception thrown if an object modified in this transaction has meanwhile been modified by another transaction.
 * In order to detect such a situation the store maintains a version counter for each (committed) version of an object.
 * Once an object is cloned into a transactional view the current version counter is also stored as original version
 * of the transactional view. Each time a transactional object is cloned back to the store of committed objects (during commit)
 * the version counter of the committed object is incremented. Before committing an object it is checked if the
 * version counter of the committed version is the same as the original version of the transactional view to commit.
 * If both version counters are the same the object has not been changed in the meantime, otherwise this exception is thrown
 *
 * @author Jan Wiemer
 */
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
