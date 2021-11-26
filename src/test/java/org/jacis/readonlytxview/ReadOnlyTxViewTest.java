/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.readonlytxview;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainerReadOnlyTransactionContext;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;

public class ReadOnlyTxViewTest {

  @Test
  public void testSimpleUsageOfReadOnlyTxContext() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(10));
      store.update("2", new TestObject("A2").setValue(20));
      store.update("3", new TestObject("A3").setValue(30));
      store.update("4", new TestObject("A4").setValue(40));
      store.update("5", new TestObject("A5").setValue(50));
    });
    AtomicReference<JacisContainerReadOnlyTransactionContext> roTxView = new AtomicReference<>();
    container.withLocalTx(() -> {
      roTxView.set(container.createReadOnlyTransactionView("TestTx2"));
    });
    JacisLocalTransaction roTx = container.beginLocalTransaction("roTx");
    container.startReadOnlyTransactionWithContext(roTxView.get());
    assertEquals(10, store.get("1").getValue());
    assertEquals(20, store.get("2").getValue());
    assertEquals(30, store.get("3").getValue());
    assertEquals(40, store.get("4").getValue());
    assertEquals(50, store.get("5").getValue());
    roTx.rollback();
  }

  @Test
  public void testUsageOfReadOnlyTxContextWithLocalModification() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(10));
      store.update("2", new TestObject("A2").setValue(20));
      store.update("3", new TestObject("A3").setValue(30));
      store.update("4", new TestObject("A4").setValue(40));
      store.update("5", new TestObject("A5").setValue(50));
    });
    AtomicReference<JacisContainerReadOnlyTransactionContext> roTxView = new AtomicReference<>();
    container.withLocalTx(() -> {
      store.update("5", store.get("5").setValue(51));
      roTxView.set(container.createReadOnlyTransactionView("TestTx2"));
    });
    JacisLocalTransaction roTx = container.beginLocalTransaction("roTx");
    container.startReadOnlyTransactionWithContext(roTxView.get());
    assertEquals(10, store.get("1").getValue());
    assertEquals(20, store.get("2").getValue());
    assertEquals(30, store.get("3").getValue());
    assertEquals(40, store.get("4").getValue());
    assertEquals(51, store.get("5").getValue());
    roTx.rollback();
  }

  @Test
  public void testUsageOfReadOnlyTxContextWithLocalModificationWithoutUpdate() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(10));
      store.update("2", new TestObject("A2").setValue(20));
      store.update("3", new TestObject("A3").setValue(30));
      store.update("4", new TestObject("A4").setValue(40));
      store.update("5", new TestObject("A5").setValue(50));
    });
    AtomicReference<JacisContainerReadOnlyTransactionContext> roTxView = new AtomicReference<>();
    container.withLocalTx(() -> {
      store.get("5").setValue(51);
      roTxView.set(container.createReadOnlyTransactionView("TestTx2"));
    });
    JacisLocalTransaction roTx = container.beginLocalTransaction("roTx");
    container.startReadOnlyTransactionWithContext(roTxView.get());
    assertEquals(10, store.get("1").getValue());
    assertEquals(20, store.get("2").getValue());
    assertEquals(30, store.get("3").getValue());
    assertEquals(40, store.get("4").getValue());
    assertEquals(51, store.get("5").getValue());
    roTx.rollback();
  }

  @Test
  public void testUsageOfReadOnlyTxContextInNestedTransaction() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(10));
      store.update("2", new TestObject("A2").setValue(20));
      store.update("3", new TestObject("A3").setValue(30));
      store.update("4", new TestObject("A4").setValue(40));
      store.update("5", new TestObject("A5").setValue(50));
    });
    AtomicReference<JacisContainerReadOnlyTransactionContext> roTxView = new AtomicReference<>();
    container.withLocalTx(() -> {
      store.get("5").setValue(51);
      roTxView.set(container.createReadOnlyTransactionView("TestTx2"));
      Thread thread = new Thread(() -> {
        JacisLocalTransaction roTx = container.beginLocalTransaction("roTx");
        container.startReadOnlyTransactionWithContext(roTxView.get());
        assertEquals(10, store.get("1").getValue());
        assertEquals(20, store.get("2").getValue());
        assertEquals(30, store.get("3").getValue());
        assertEquals(40, store.get("4").getValue());
        assertEquals(51, store.get("5").getValue());
        roTx.rollback();
      });
      thread.start();
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }

}