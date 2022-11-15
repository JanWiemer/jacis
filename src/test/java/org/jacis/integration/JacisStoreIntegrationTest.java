/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.store.JacisTransactionInfo;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("CodeBlock2Expr")
public class JacisStoreIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreIntegrationTest.class);

  @Test
  public void testPreventDirtyRead() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    // INIT
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update(testObjectName, testObject);
    initTx.commit();
    // UPDATE (without commit)
    JacisLocalTransaction updatingTx = store.getContainer().beginLocalTransaction();
    TestObject obj2update = store.get(testObjectName);
    obj2update.setValue(2);
    store.update(testObjectName, obj2update);
    JacisTransactionHandle updatingTxHandle = testHelper.suspendTx();
    // READ
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(1, store.get(testObjectName).getValue()); // ===== READ => committed value ====
    readingTx.commit();
    // COMMIT
    testHelper.resumeTx(updatingTxHandle);
    updatingTx.commit();
    // READ COMMITTED
    JacisLocalTransaction checkingTx = store.getContainer().beginLocalTransaction();
    assertEquals(2, store.get(testObjectName).getValue()); // ===== READ => committed value ====
    checkingTx.commit();
  }

  @Test
  public void testPreventNonRepeatableRead() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    // INIT
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update(testObjectName, testObject);
    initTx.commit();
    // READ 1
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(1, store.get(testObjectName).getValue()); // ===== READ => committed value ====
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // UPDATE (with commit)
    JacisLocalTransaction updatingTx = store.getContainer().beginLocalTransaction();
    TestObject obj2update = store.get(testObjectName);
    obj2update.setValue(2);
    store.update(testObjectName, obj2update);
    updatingTx.commit();
    // REPEATED READ
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.get(testObjectName).getValue()); // ===== READ => same value as first read ====
    readingTx.commit();
    // READ COMMITTED IN NEW TX
    JacisLocalTransaction checkingTx = store.getContainer().beginLocalTransaction();
    assertEquals(2, store.get(testObjectName).getValue()); // ===== READ => committed value ====
    checkingTx.commit();
  }

  @Test
  public void testTransactionIsolation() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    // READABLE -> object not contained
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE insert
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, testObject); // <--------------------
    assertTrue(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // READABLE -> object not contained
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE commit
    testHelper.resumeTx(writingTxHandle);
    writingTx.prepare(); // <--------------------
    writingTx.commit(); // <--------------------
    // READABLE -> object found
    testHelper.resumeTx(readingTxHandle);
    assertTrue(store.containsKey(testObjectName));
    readingTx.commit();
  }

  @Test
  public void testCoreStoreGarbageOnCommit() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    TestObject testObject = store.get("testObj");
    assertNull(testObject);
    assertEquals(1, store.size()); // a shadow core object for the null object in the TX view exists
    writingTx.commit(); // <--------------------
    assertEquals(0, store.size()); // the shadow core object is cleaned up
  }

  @Test
  public void testCoreStoreGarbageOnRollback() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    TestObject testObject = store.get("testObj");
    assertNull(testObject);
    assertEquals(1, store.size()); // a shadow core object for the null object in the TX view exists
    writingTx.rollback(); // <--------------------
    assertEquals(0, store.size()); // the shadow core object is cleaned up
  }

  @Test
  public void testCoreStoreGarbageOnCommitOfLastTx() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx1 = store.getContainer().beginLocalTransaction();
    assertNull(store.get("testObj"));
    assertEquals(1, store.size()); // a shadow core object for the null object in the TX view exists
    JacisTransactionHandle txHandle1 = testHelper.suspendTx();
    JacisLocalTransaction writingTx2 = store.getContainer().beginLocalTransaction();
    assertNull(store.get("testObj"));
    assertEquals(1, store.size()); // again the same shador core object
    writingTx2.commit(); // <--------------------
    assertEquals(1, store.size()); // still referenced in TX1
    testHelper.resumeTx(txHandle1);
    assertEquals(1, store.size()); // still referenced in TX1
    writingTx1.commit(); // <--------------------
    assertEquals(0, store.size()); // the shadow core object is cleaned up
  }

  @Test
  public void testTransactionInfo() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisLocalTransaction tx = store.getContainer().beginLocalTransaction();
    JacisTransactionInfo info = container.getTransactionInfo(tx);
    assertNotNull(info);
    assertTrue(info.getStoreTxInfos().isEmpty());
    log.info("TX1-initial:       {}", info);
    // --------------------
    store.update(testObjectName, testObject);
    info = container.getTransactionInfo(tx);
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(false, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX1-after-update:  {}", info);
    // --------------------
    tx.prepare(); // <--------------------
    info = container.getTransactionInfo(tx);
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(true, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX1-after-prepare: {}", info);
    // --------------------
    tx.commit(); // <--------------------
    info = container.getTransactionInfo(tx);
    assertNull(info);
    log.info("TX1-after-commit:  {}", info);
    // --------------------
    // --------------------
    tx = store.getContainer().beginLocalTransaction();
    info = container.getTransactionInfo(tx);
    assertNotNull(info);
    assertTrue(info.getStoreTxInfos().isEmpty());
    log.info("TX2-initial:       {}", info);
    // --------------------
    sleep(10);
    store.get(testObjectName);
    info = container.getTransactionInfo(tx);
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(0, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(false, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX2-after-read:  {}", info);
    // --------------------
    sleep(10);
    store.update(testObjectName, testObject);
    info = container.getTransactionInfo(tx);
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(false, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX2-after-update:  {}", info);
    // --------------------
    sleep(10);
    tx.commit(); // <--------------------
    info = container.getTransactionInfo(tx);
    assertNull(info);
    log.info("TX2-after-commit:  {}", info);
    // --------------------
    info = container.getLastFinishedTransactionInfo();
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(false, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX2-last-after-commit:  {}", info);
  }

  @Test
  public void testGetCommittedVersion() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction tx = store.getContainer().beginLocalTransaction();
    store.update(testObjectName, testObject);
    tx.commit();
    // --------------------
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    // --------------------
    tx = store.getContainer().beginLocalTransaction();
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    store.getReadOnly(testObjectName);
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    TestObject testObjTxView = store.get(testObjectName);
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    testObjTxView.setValue(2);
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    store.update(testObjectName, testObjTxView);
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    tx.prepare();
    assertEquals(1, store.getCommittedValue(testObjectName).getValue());
    tx.commit();
    assertEquals(2, store.getCommittedValue(testObjectName).getValue());
    // --------------------
    // --------------------
    tx = store.getContainer().beginLocalTransaction();
    assertEquals(2, store.getCommittedValue(testObjectName).getValue());
    store.getReadOnly(testObjectName);
    assertEquals(2, store.getCommittedValue(testObjectName).getValue());
    testObjTxView = store.get(testObjectName);
    assertEquals(2, store.getCommittedValue(testObjectName).getValue());
    testObjTxView.setValue(3);
    assertEquals(2, store.getCommittedValue(testObjectName).getValue());
    store.update(testObjectName, testObjTxView);
    JacisTransactionHandle txHandle1 = testHelper.suspendTx();
    JacisLocalTransaction tx2 = store.getContainer().beginLocalTransaction();
    TestObject testObjTxView2 = store.get(testObjectName);
    testObjTxView2.setValue(4);
    store.update(testObjectName, testObjTxView2);
    tx2.commit();
    testHelper.resumeTx(txHandle1);
    assertEquals(4, store.getCommittedValue(testObjectName).getValue());
    assertEquals(2, store.getTransactionStartValue(testObjectName).getValue());
    assertEquals(3, store.getReadOnly(testObjectName).getValue());
    assertEquals(3, store.get(testObjectName).getValue());
    assertEquals(4, store.refresh(testObjectName).getValue());
    assertEquals(4, store.get(testObjectName).getValue());
    tx.prepare();
    tx.commit();
    assertEquals(4, store.getCommittedValue(testObjectName).getValue());
    // --------------------
  }

  @Test
  public void testReadOnlySnapshot() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction writingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle writingTxHandle = testHelper.suspendTx();
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle readingTxHandle = testHelper.suspendTx();
    //
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot without tx
    testHelper.resumeTx(readingTxHandle);
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot in reading tx
    testHelper.suspendTx();
    testHelper.resumeTx(writingTxHandle);
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot in writing tx
    testHelper.suspendTx();
    //
    // insert
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, testObject); // <--------------------
    testHelper.suspendTx();
    //
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot without tx
    testHelper.resumeTx(readingTxHandle);
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot in reading tx
    testHelper.suspendTx();
    testHelper.resumeTx(writingTxHandle);
    assertEquals(0, store.getReadOnlySnapshot().size()); // snapshot in writing tx
    testHelper.suspendTx();
    //
    // commit
    testHelper.resumeTx(writingTxHandle);
    store.update(testObjectName, testObject); // <--------------------
    writingTx.prepare(); // <--------------------
    writingTx.commit(); // <--------------------
    //
    assertEquals(1, store.getReadOnlySnapshot().size()); // snapshot without tx
    testHelper.resumeTx(readingTxHandle);
    assertEquals(1, store.getReadOnlySnapshot().size()); // snapshot in reading tx
    testHelper.suspendTx();
    testHelper.resumeTx(writingTxHandle);
    assertEquals(1, store.getReadOnlySnapshot().size()); // snapshot in writing tx
    testHelper.suspendTx();
    //
    testHelper.resumeTx(readingTxHandle);
    readingTx.commit();
  }

  @Test
  public void testNonTransactionInit() {
    int n = 10000;
    List<TestObject> objects = new ArrayList<>(n);
    for (int i = 1; i <= n; i++) {
      objects.add(new TestObject("obj-" + i));
    }
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.initStoreNonTransactional(objects, o -> o.getName(), 5);
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertTrue(store.containsKey("obj-1"));
    assertEquals(n, store.size());
    readingTx.commit();
  }

  @Test
  public void testReadOnlyWithoutOptimisticLocking() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction tx0 = store.getContainer().beginLocalTransaction();
    store.update(testObjectName, testObject);
    tx0.commit();
    assertEquals(1, store.getReadOnly(testObjectName).getValue());
    //
    JacisLocalTransaction tx1 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle tx1Handle = testHelper.suspendTx();
    JacisLocalTransaction tx2 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle tx2Handle = testHelper.suspendTx();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(1, store.getReadOnly(testObjectName).getValue());
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx2Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(2));
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(1, store.getReadOnly(testObjectName).getValue());
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx2Handle);
    tx2.commit();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(2, store.getReadOnly(testObjectName).getValue()); // tx2 committed, read only not cloning -> updated value
    tx1.commit();
  }

  @Test
  public void testReadOnlyWithOptimisticLocking() {
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisLocalTransaction tx0 = store.getContainer().beginLocalTransaction();
    store.update(testObjectName, testObject);
    tx0.commit();
    assertEquals(1, store.getReadOnly(testObjectName).getValue());
    //
    JacisLocalTransaction tx1 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle tx1Handle = testHelper.suspendTx();
    JacisLocalTransaction tx2 = store.getContainer().beginLocalTransaction();
    JacisTransactionHandle tx2Handle = testHelper.suspendTx();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(1, store.lockReadOnly(testObjectName).getValue()); // OPTIMISTIC LOCKED
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx2Handle);
    store.update(testObjectName, store.get(testObjectName).setValue(2));
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(1, store.getReadOnly(testObjectName).getValue());
    testHelper.suspendTx();
    //
    testHelper.resumeTx(tx2Handle);
    tx2.commit();
    //
    testHelper.resumeTx(tx1Handle);
    assertEquals(2, store.getReadOnly(testObjectName).getValue());
    // tx2 committed, read only not cloning -> updated value
    // BUT object locked -> STALE expected!
    try {
      tx1.commit();
      throw new IllegalStateException("Commit should throw stale because object was locked optimistically!");
    } catch (JacisStaleObjectException e) { // expected
      log.info("Caught expected exception: " + e);
    }
  }

  protected void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
