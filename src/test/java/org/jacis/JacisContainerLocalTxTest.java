/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class JacisContainerLocalTxTest {

  private static final Logger log = LoggerFactory.getLogger(JacisContainerLocalTxTest.class);

  private void assertNoTransaction(JacisContainer container) {
    try {
      JacisTransactionHandle tx = container.getCurrentTransaction(true);
      fail("No teansaction expected but found: " + tx);
    } catch (JacisNoTransactionException e) {
      // expected
    }
  }

  @Test
  public void testInitiallyNoTransaction() {
    JacisContainer container = new JacisContainer();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    assertFalse(container.isInTransaction());
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    assertTrue(container.isInTransaction());
    tx.commit();
    assertFalse(container.isInTransaction());
    assertNoTransaction(container);
  }

  @Test
  public void testBeginRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.rollback();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginPrepareCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.prepare();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.commit();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginPrepareRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.prepare();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.rollback();
    assertNoTransaction(container);
  }

  @Test(expected = JacisTransactionAlreadyStartedException.class)
  public void testBeginBegin() {
    JacisContainer container = new JacisContainer();
    container.beginLocalTransaction("test");
    container.beginLocalTransaction("test");
  }

  @Test(expected = JacisTransactionAlreadyStartedException.class)
  public void testBeginPrepareBegin() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    tx.prepare();
    container.beginLocalTransaction("test");
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginCommitCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    tx.commit();
    tx.commit();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginCommitRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    tx.commit();
    tx.rollback();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginRollbackCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    tx.rollback();
    tx.commit();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginRollbackRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    tx.rollback();
    tx.rollback();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testInsertWithoutTransaction() {
    JacisContainer container = new JacisContainer();
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning(container);
    store.update("obj-1", new TestObject("obj-1", 1));
  }

  @Test()
  public void testInsertWithinTransaction() {
    JacisContainer container = new JacisContainer();
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning(container);
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    store.update("obj-1", new TestObject("obj-1", 1));
    tx.commit();
  }

  @Test()
  public void testTransactionDescription() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction("test");
    log.info("Transaction: {}", tx);
    assertTrue(tx.getTxName().contains(JacisContainerLocalTxTest.class.getName()));
    tx.commit();
    container.withLocalTxAndRetry(5, () -> {
      JacisTransactionHandle tx2 = container.getCurrentTransaction(false);
      log.info("Transaction2: {}", tx2);
      assertTrue(tx2.getTxName().contains(JacisContainerLocalTxTest.class.getName()));
    });
  }

}
