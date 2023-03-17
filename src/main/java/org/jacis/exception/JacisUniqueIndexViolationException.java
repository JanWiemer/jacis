/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;

/**
 * Thrown when trying to commit an object that would violate a unique index
 * (or registering a unique index that would be violated by the existing objects).
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisUniqueIndexViolationException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public JacisUniqueIndexViolationException(String s) {
    super(s);
  }
}
