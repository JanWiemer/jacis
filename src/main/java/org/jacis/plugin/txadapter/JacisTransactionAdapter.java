/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;

/**
 * @author Jan Wiemer
 * 
 * Transaction adapter that can be registered to bind the Jacis Store to externally managed transactions.
 */
public interface JacisTransactionAdapter {

  JacisTransactionHandle getCurrentTransaction(boolean enforce) throws JacisNoTransactionException;

  void joinCurrentTransaction(JacisTransactionHandle tramsaction, JacisContainer container);

  void destroyCurrentTransaction();

}
