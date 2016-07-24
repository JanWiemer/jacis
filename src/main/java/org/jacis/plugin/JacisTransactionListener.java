/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * @author Jan Wiemer
 *
 * Callback methods before and after prepare, commit and rollback.
 */
public interface JacisTransactionListener {

  void beforePrepare(JacisContainer container, JacisTransactionHandle tx);

  void afterPrepare(JacisContainer container, JacisTransactionHandle tx);

  void beforeCommit(JacisContainer container, JacisTransactionHandle tx);

  void afterCommit(JacisContainer container, JacisTransactionHandle tx);

  void beforeRollback(JacisContainer container, JacisTransactionHandle tx);

  void afterRollback(JacisContainer container, JacisTransactionHandle tx);

}
