package org.jacis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.txadapter.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;

public class JacisContainerLocalTxTest {

  protected void assertNoTransaction(JacisContainer container) {
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
    JacisLocalTransaction tx = container.beginLocalTransaction();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.commit();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.rollback();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginPrepareCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.prepare();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.commit();
    assertNoTransaction(container);
  }

  @Test
  public void testBeginPrepareRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.prepare();
    assertEquals(tx, container.getCurrentTransaction(true).getExternalTransaction());
    tx.rollback();
    assertNoTransaction(container);
  }

  @Test(expected = JacisTransactionAlreadyStartedException.class)
  public void testBeginBegin() {
    JacisContainer container = new JacisContainer();
    container.beginLocalTransaction();
    container.beginLocalTransaction();
  }

  @Test(expected = JacisTransactionAlreadyStartedException.class)
  public void testBeginPrepareBegin() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    tx.prepare();
    container.beginLocalTransaction();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginCommitCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    tx.commit();
    tx.commit();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginCommitRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    tx.commit();
    tx.rollback();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginRollbackCommit() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    tx.rollback();
    tx.commit();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testBeginRollbackRollback() {
    JacisContainer container = new JacisContainer();
    JacisLocalTransaction tx = container.beginLocalTransaction();
    tx.rollback();
    tx.rollback();
  }

  @Test(expected = JacisNoTransactionException.class)
  public void testInsertWithoutTransaction() {
    JacisContainer container = new JacisContainer();
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStore(container);
    store.update("obj-1", new TestObject("obj-1", 1));
  }

  @Test()
  public void testInsertWithinTransaction() {
    JacisContainer container = new JacisContainer();
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStore(container);
    JacisLocalTransaction tx = container.beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    tx.commit();
  }

}
