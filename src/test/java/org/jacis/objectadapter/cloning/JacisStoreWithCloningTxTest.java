/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.objectadapter.cloning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("CodeBlock2Expr")
public class JacisStoreWithCloningTxTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreWithCloningTxTest.class);

  @Test
  public void testTransactionIsolationInsertPrepareCommit() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, testObject); // <--------------------
    assertTrue(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    writingTx.prepare(); // <--------------------
    assertTrue(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    writingTx.commit(); // <--------------------
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertTrue(store.containsKey(testObjectName));
    readingTx.commit();
  }

  @Test
  public void testTransactionIsolationUpdatePrepareCommit() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue());
    assertEquals(1, store.refresh(testObjectName).getValue());
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, store.get(testObjectName).setValue(2)); // <--------------------
    assertEquals(2, store.get(testObjectName).getValue());
    testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue());
    assertEquals(1, store.refresh(testObjectName).getValue());
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    writingTx.prepare(); // <--------------------
    assertEquals(2, store.get(testObjectName).getValue());
    testHelper.suspendTx();
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue());
    assertEquals(1, store.refresh(testObjectName).getValue());
    testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    writingTx.commit(); // <--------------------
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue()); // consistency! Value already seen by readable TX
    assertEquals(2, store.refresh(testObjectName).getValue());
    readingTx.commit();
  }

  @Test
  public void testTransactionIsolationInsertRollback() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, testObject); // <--------------------
    assertTrue(store.containsKey(testObjectName));
    writingTx.rollback(); // <--------------------
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    readingTx.commit();
  }

  @Test
  public void testTransactionIsolationUpdateRollback() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // WRITABLE
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, store.get(testObjectName).setValue(2)); // <--------------------
    assertEquals(2, store.get(testObjectName).getValue());
    writingTx.rollback(); // <--------------------
    // READABLE
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue()); // consistency! Value already seen by readable TX
    assertEquals(1, store.refresh(testObjectName).getValue());
    readingTx.commit();
  }

  @Test(expected = JacisStaleObjectException.class)
  public void testStaleObjectAfterCommit() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    JacisLocalTransaction writingTx1 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTx1Handle = testHelper.suspendTx();
    JacisLocalTransaction writingTx2 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTx2Handle = testHelper.suspendTx();
    // TX 1
    testHelper.resumeTx(writingTx1Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(2)); // <--------------------
    testHelper.suspendTx();
    // TX 2
    testHelper.resumeTx(writingTx2Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(3)); // <--------------------
    testHelper.suspendTx();
    // TX 1
    testHelper.resumeTx(writingTx1Handle);
    writingTx1.prepare(); // <--------------------
    writingTx1.commit(); // <--------------------
    // TX 1
    testHelper.resumeTx(writingTx2Handle);
    try {
      assertTrue(store.isStale(testObjectName));
      writingTx2.prepare(); // <--------------------
    } catch (JacisStaleObjectException e) {
      log.info("Caught expected exception {}", "" + e);
      throw e;
    }
  }

  @Test(expected = JacisStaleObjectException.class)
  public void testStaleObjectAfterPrepare() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    JacisLocalTransaction writingTx1 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTx1Handle = testHelper.suspendTx();
    JacisLocalTransaction writingTx2 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTx2Handle = testHelper.suspendTx();
    // TX 1
    testHelper.resumeTx(writingTx1Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(2)); // <--------------------
    testHelper.suspendTx();
    // TX 2
    testHelper.resumeTx(writingTx2Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(3)); // <--------------------
    testHelper.suspendTx();
    // TX 1
    testHelper.resumeTx(writingTx1Handle);
    writingTx1.prepare(); // <--------------------
    testHelper.suspendTx();
    // TX 1
    testHelper.resumeTx(writingTx2Handle);
    try {
      assertTrue(store.isStale(testObjectName));
      writingTx2.prepare(); // <--------------------
    } catch (JacisStaleObjectException e) {
      log.info("Caught expected exception {}", "" + e);
      throw e;
    }
  }

}
