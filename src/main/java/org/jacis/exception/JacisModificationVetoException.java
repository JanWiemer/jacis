/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;
import org.jacis.plugin.JacisModificationListener;

/**
 * Thrown when a {@link JacisModificationListener} has a veto against a tracked modification during the prepare phase.
 * In this case it can throw this exception to rollback the whole transaction
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisModificationVetoException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  public JacisModificationVetoException(JacisModificationListener<?,?> modListener, String message) {
    super("Modification veto by "+modListener.getClass().getSimpleName()+": "+message);
  }
}
