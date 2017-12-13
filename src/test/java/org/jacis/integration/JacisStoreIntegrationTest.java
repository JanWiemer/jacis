/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
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
    store.update(testObjectName, testObject); //<--------------------
    assertTrue(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // READABLE -> object not contained
    testHelper.resumeTx(readingTxHandle);
    assertFalse(store.containsKey(testObjectName));
    testHelper.suspendTx();
    // WRITABLE commit
    testHelper.resumeTx(writingTxHandle);
    writingTx.prepare(); //<--------------------
    writingTx.commit(); //<--------------------
    // READABLE -> object found
    testHelper.resumeTx(readingTxHandle);
    assertTrue(store.containsKey(testObjectName));
    readingTx.commit();
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
    tx.prepare(); //<--------------------
    info = container.getTransactionInfo(tx);
    assertEquals(1, info.getStoreTxInfos().size());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfTxViewEntries());
    assertEquals(1, info.getStoreTxInfos().get(0).getNumberOfUpdatedTxViewEntries());
    assertEquals(true, info.getStoreTxInfos().get(0).isCommitPending());
    log.info("TX1-after-prepare: {}", info);
    // --------------------
    tx.commit(); //<--------------------
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
    tx.commit(); //<--------------------    
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

  protected void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
