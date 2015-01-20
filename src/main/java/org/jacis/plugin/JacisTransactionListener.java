package org.jacis.plugin;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;

/**
 * @author Jan Wiemer
 *
 * Callback methods before and after prepare, commit and rollback.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public interface JacisTransactionListener {

  public void beforePrepare(JacisContainer container, JacisTransactionHandle tx);

  public void afterPrepare(JacisContainer container, JacisTransactionHandle tx);

  public void beforeCommit(JacisContainer container, JacisTransactionHandle tx);

  public void afterCommit(JacisContainer container, JacisTransactionHandle tx);

  public void beforeRollback(JacisContainer container, JacisTransactionHandle tx);

  public void afterRollback(JacisContainer container, JacisTransactionHandle tx);

}
