/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisUniqueIndexViolationException;
import org.jacis.integration.JacisStoreIntegrationTest;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacisUniqueIndexTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreIntegrationTest.class);

  @Test
  public void testUniqueIndexAccessorMethods() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisUniqueIndex<String, String, TestObject> index = store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    JacisUniqueIndex<String, String, TestObject> sameindex = store.getUniqueIndex("IDX-NAME");
    log.info("index1: {} ", index);
    log.info("index2: {} ", sameindex);
    assertSame(index, sameindex);
  }

  @Test(expected = IllegalStateException.class)
  public void testUniqueIndexCreateIndexTwoTimes() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    store.createUniqueIndex("IDX-NAME", o -> o.getValue());
  }

  @Test
  public void testUniqueIndexSimpleAccess() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
      store.update("3", new TestObject("A3").setValue(5).setStrValue("IDX-3"));
      store.update("4", new TestObject("A4").setValue(5).setStrValue("IDX-4"));
    });
    assertEquals("A1", index.getReadOnly("IDX-1").getName());
    assertEquals("A2", index.getReadOnly("IDX-2").getName());
    assertEquals("A3", index.getReadOnly("IDX-3").getName());
    assertEquals("A4", index.getReadOnly("IDX-4").getName());
    container.withLocalTx(() -> {
      assertEquals("A1", index.get("IDX-1").getName());
      assertEquals("A2", index.get("IDX-2").getName());
      assertEquals("A3", index.get("IDX-3").getName());
      assertEquals("A4", index.get("IDX-4").getName());
    });
  }

  @Test
  public void testUniqueIndexCreateWhileObjectsExist() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("3", new TestObject("A3").setValue(5).setStrValue("IDX-3"));
      store.update("4", new TestObject("A4").setValue(5).setStrValue("IDX-4"));
    });
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    assertEquals("A1", index.getReadOnly("IDX-1").getName());
    assertEquals("A2", index.getReadOnly("IDX-2").getName());
    assertEquals("A3", index.getReadOnly("IDX-3").getName());
    assertEquals("A4", index.getReadOnly("IDX-4").getName());
    container.withLocalTx(() -> {
      assertEquals("A1", index.get("IDX-1").getName());
      assertEquals("A2", index.get("IDX-2").getName());
      assertEquals("A3", index.get("IDX-3").getName());
      assertEquals("A4", index.get("IDX-4").getName());
    });
  }

  @Test
  public void testUniqueIndexWhileModifyedInTransaction() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1a"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2a"));
    });
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      assertEquals("A1", index.getReadOnly("IDX-1a").getName());
      assertNull(index.getReadOnly("IDX-1b"));
      assertNull(index.getReadOnly("IDX-1c"));
      //
      store.update("1", new TestObject("A1").setStrValue("IDX-1b"));
      assertEquals("A1", index.getReadOnly("IDX-1b").getName());
      assertNull(index.getReadOnly("IDX-1a"));
      assertNull(index.getReadOnly("IDX-1c"));
      //
      store.update("1", new TestObject("A1").setStrValue("IDX-1c"));
      assertNull(index.getReadOnly("IDX-1a"));
      assertNull(index.getReadOnly("IDX-1b"));
      assertEquals("A1", index.getReadOnly("IDX-1c").getName());
      store.update("1", new TestObject("A1").setStrValue("IDX-1b"));
      assertNull(index.getReadOnly("IDX-1a"));
      assertEquals("A1", index.getReadOnly("IDX-1b").getName());
      assertNull(index.getReadOnly("IDX-1c"));
    });
    assertNull(index.getReadOnly("IDX-1a"));
    assertEquals("A1", index.getReadOnly("IDX-1b").getName());
    assertNull(index.getReadOnly("IDX-1c"));
  }

  @Test(expected = JacisUniqueIndexViolationException.class)
  public void testUniqueIndexViolationInInit() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-1"));
    });
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
  }

  @Test(expected = JacisUniqueIndexViolationException.class)
  public void testUniqueIndexViolationInUpdate() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-1"));
    });
  }

  @Test(expected = JacisUniqueIndexViolationException.class)
  public void testUniqueIndexViolationInUpdate2() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    container.withLocalTx(() -> {
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-1"));
    });
  }

  @Test()
  public void testUniqueIndexViolationInCommit() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    JacisLocalTransaction tx1 = container.beginLocalTransaction("TX1");
    store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
    JacisTransactionHandle suspendTx1 = testHelper.suspendTx();
    JacisLocalTransaction tx2 = container.beginLocalTransaction("TX2");
    store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
    tx2.prepare();
    tx2.commit();
    testHelper.resumeTx(suspendTx1);
    try {
      tx1.prepare();
      fail("expected " + JacisUniqueIndexViolationException.class + " to be thrown");
    } catch (JacisUniqueIndexViolationException e) {
      // expected
    }
  }

  @Test()
  public void testUniqueIndexViolationInCommit2() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", o -> o.getStrValue());
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    JacisLocalTransaction tx1 = container.beginLocalTransaction("TX1");
    store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
    JacisTransactionHandle suspendTx1 = testHelper.suspendTx();
    JacisLocalTransaction tx2 = container.beginLocalTransaction("TX2");
    store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
    tx2.prepare();
    JacisTransactionHandle suspendTx2 = testHelper.suspendTx();
    testHelper.resumeTx(suspendTx1);
    try {
      tx1.prepare();
      fail("expected " + JacisUniqueIndexViolationException.class + " to be thrown");
    } catch (JacisUniqueIndexViolationException e) {
      // expected
      tx1.rollback();
    }
    testHelper.resumeTx(suspendTx2);
    tx2.commit();
  }

}