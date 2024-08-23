/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.index;

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

import java.util.List;

import static org.junit.Assert.*;

public class JacisUniqueIndexTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreIntegrationTest.class);

  @Test
  public void testUniqueIndexAccessorMethods() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisUniqueIndex<String, String, TestObject> index = store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
    JacisUniqueIndex<String, String, TestObject> sameIndex = store.getUniqueIndex("IDX-NAME");
    log.info("index1: {} ", index);
    log.info("index2: {} ", sameIndex);
    assertSame(index, sameIndex);
  }

  @Test(expected = IllegalStateException.class)
  public void testUniqueIndexCreateIndexTwoTimes() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
    store.createUniqueIndex("IDX-NAME", TestObject::getValue);
  }

  @Test
  public void testUniqueIndexSimpleAccess() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
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
    assertEquals(0, index.multiGetReadOnly(List.of("IDX-0")).size());
    assertEquals(1, index.multiGetReadOnly(List.of("IDX-1")).size());
    assertEquals(1, index.multiGetReadOnly(List.of("IDX-2")).size());
    assertEquals(1, index.multiGetReadOnly(List.of("IDX-3")).size());
    assertEquals(1, index.multiGetReadOnly(List.of("IDX-4")).size());
    assertEquals(4, index.multiGetReadOnly(List.of("IDX-1", "IDX-2", "IDX-3", "IDX-4")).size());
    assertEquals(0, index.streamReadOnly(List.of("IDX-0")).count());
    assertEquals(1, index.streamReadOnly(List.of("IDX-1")).count());
    assertEquals(1, index.streamReadOnly(List.of("IDX-2")).count());
    assertEquals(1, index.streamReadOnly(List.of("IDX-3")).count());
    assertEquals(1, index.streamReadOnly(List.of("IDX-4")).count());
    assertEquals(4, index.streamReadOnly(List.of("IDX-1", "IDX-2", "IDX-3", "IDX-4")).count());
    container.withLocalTx(() -> {
      assertEquals("A1", index.get("IDX-1").getName());
      assertEquals("A2", index.get("IDX-2").getName());
      assertEquals("A3", index.get("IDX-3").getName());
      assertEquals("A4", index.get("IDX-4").getName());
      assertEquals(0, index.multiGet(List.of("IDX-0")).size());
      assertEquals(1, index.multiGet(List.of("IDX-1")).size());
      assertEquals(1, index.multiGet(List.of("IDX-2")).size());
      assertEquals(1, index.multiGet(List.of("IDX-3")).size());
      assertEquals(1, index.multiGet(List.of("IDX-4")).size());
      assertEquals(4, index.multiGet(List.of("IDX-1", "IDX-2", "IDX-3", "IDX-4")).size());

      assertEquals(0, index.stream(List.of("IDX-0")).count());
      assertEquals(1, index.stream(List.of("IDX-1")).count());
      assertEquals(1, index.stream(List.of("IDX-2")).count());
      assertEquals(1, index.stream(List.of("IDX-3")).count());
      assertEquals(1, index.stream(List.of("IDX-4")).count());
      assertEquals(4, index.stream(List.of("IDX-1", "IDX-2", "IDX-3", "IDX-4")).count());
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
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
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
  public void testUniqueIndexWhileModifiedInTransaction() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1a"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2a"));
    });
    JacisUniqueIndex<Object, String, TestObject> index = store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      assertEquals("A1", index.getReadOnly("IDX-1a").getName());
      assertNull(index.getReadOnly("IDX-1b"));
      assertNull(index.getReadOnly("IDX-1c"));
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).size());
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1a")).size());
      assertEquals(0, index.multiGetReadOnly(List.of("IDX-1b", "IDX-1c")).size());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).count());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1a")).count());
      assertEquals(0, index.streamReadOnly(List.of("IDX-1b", "IDX-1c")).count());
      //
      store.update("1", new TestObject("A1").setStrValue("IDX-1b"));
      assertNull(index.getReadOnly("IDX-1a"));
      assertEquals("A1", index.getReadOnly("IDX-1b").getName());
      assertNull(index.getReadOnly("IDX-1c"));
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).size());
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1b")).size());
      assertEquals(0, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1c")).size());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).count());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1b")).count());
      assertEquals(0, index.streamReadOnly(List.of("IDX-1a", "IDX-1c")).count());
      //
      store.update("1", new TestObject("A1").setStrValue("IDX-1c"));
      assertNull(index.getReadOnly("IDX-1a"));
      assertNull(index.getReadOnly("IDX-1b"));
      assertEquals("A1", index.getReadOnly("IDX-1c").getName());
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).size());
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1c")).size());
      assertEquals(0, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1b")).size());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).count());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1c")).count());
      assertEquals(0, index.streamReadOnly(List.of("IDX-1a", "IDX-1b")).count());
      //
      store.update("1", new TestObject("A1").setStrValue("IDX-1b"));
      assertNull(index.getReadOnly("IDX-1a"));
      assertEquals("A1", index.getReadOnly("IDX-1b").getName());
      assertNull(index.getReadOnly("IDX-1c"));
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).size());
      assertEquals(1, index.multiGetReadOnly(List.of("IDX-1b")).size());
      assertEquals(0, index.multiGetReadOnly(List.of("IDX-1a", "IDX-1c")).size());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1a", "IDX-1b", "IDX-1c")).count());
      assertEquals(1, index.streamReadOnly(List.of("IDX-1b")).count());
      assertEquals(0, index.streamReadOnly(List.of("IDX-1a", "IDX-1c")).count());
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
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
  }

  @Test(expected = JacisUniqueIndexViolationException.class)
  public void testUniqueIndexViolationInUpdate() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
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
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    container.withLocalTx(() -> store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-1")));
  }

  @Test()
  public void testUniqueIndexViolationInCommit() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
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
    store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
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

  @Test
  public void testUniqueIndexClearedDuringClear() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisUniqueIndex<Object, String, TestObject> idx = store.createUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1")));
    assertNotNull(idx.getReadOnly("IDX-1"));
    container.withLocalTx(store::clear);
    assertNull(idx.getReadOnly("IDX-1"));
  }

  @Test
  public void testUniqueIndexAddDuringTx() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
    });
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      store.update("3", new TestObject("A3").setValue(5).setStrValue("IDX-0"));
      assertEquals(3, index.getReadOnly("IDX-0").size());
    });
    assertEquals(3, index.getReadOnly("IDX-0").size());
  }

  @Test
  public void testUniqueIndexRemoveDuringTx() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
    });
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      store.remove("2");
      log.info("index.getReadOnly(IDX-0)={} after remove", index.getReadOnly("IDX-0"));
      assertEquals(1, index.getReadOnly("IDX-0").size());
    });
    assertEquals(1, index.getReadOnly("IDX-0").size());
  }

  @Test
  public void testUniqueIndexRemoveByChangeDuringTx() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
    });
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      TestObject obj = store.get("2").setStrValue("OTHER");
      assertEquals(2, index.getReadOnly("IDX-0").size());
      store.update("2", obj);
      assertEquals(1, index.getReadOnly("IDX-0").size());
    });
    assertEquals(1, index.getReadOnly("IDX-0").size());
  }

  @Test
  public void testUniqueIndexAddByChangeDuringTx() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-0"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-0"));
      store.update("3", new TestObject("A2").setValue(5).setStrValue("OTHER"));
    });
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      TestObject obj = store.get("3").setStrValue("IDX-0");
      assertEquals(2, index.getReadOnly("IDX-0").size());
      store.update("3", obj);
      assertEquals(3, index.getReadOnly("IDX-0").size());
    });
    assertEquals(3, index.getReadOnly("IDX-0").size());
  }

}
