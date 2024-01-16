/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.index;

import org.jacis.container.JacisContainer;
import org.jacis.integration.JacisStoreIntegrationTest;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class JacisNonUniqueIndexTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreIntegrationTest.class);

  @Test
  public void testNonUniqueIndexAccessorMethods() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisNonUniqueIndex<String, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    JacisNonUniqueIndex<String, String, TestObject> sameIndex = store.getNonUniqueIndex("IDX-NAME");
    log.info("index1: {} ", index);
    log.info("index2: {} ", sameIndex);
    assertSame(index, sameIndex);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonUniqueIndexCreateIndexTwoTimes() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    store.createNonUniqueIndex("IDX-NAME", TestObject::getValue);
  }

  @Test
  public void testNonUniqueIndexSimpleAccess() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1a").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A1b").setValue(5).setStrValue("IDX-1"));
      store.update("3", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
      store.update("4", new TestObject("A3").setValue(5).setStrValue("IDX-3"));
    });
    assertEquals(2, index.getReadOnly("IDX-1").size());
    assertEquals(1, index.getReadOnly("IDX-2").size());
    assertEquals(1, index.getReadOnly("IDX-3").size());
    assertEquals("A2", index.getReadOnly("IDX-2").iterator().next().getName());
    assertEquals("A3", index.getReadOnly("IDX-3").iterator().next().getName());
    assertEquals(3, index.multiGetReadOnly(Arrays.asList("IDX-1", "IDX-2")).size());
    log.info("IDX-1: {}", index.getReadOnly("IDX-1"));
    log.info("IDX-2: {}", index.getReadOnly("IDX-2"));
    log.info("IDX-3: {}", index.getReadOnly("IDX-3"));
    container.withLocalTx(() -> {
      assertEquals(2, index.get("IDX-1").size());
      assertEquals(1, index.get("IDX-2").size());
      assertEquals(1, index.get("IDX-3").size());
      assertEquals("A2", index.get("IDX-2").iterator().next().getName());
      assertEquals("A3", index.get("IDX-3").iterator().next().getName());
      assertEquals(3, index.multiGet(Arrays.asList("IDX-1", "IDX-2")).size());
    });
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
    });
    assertEquals("A1", index.getReadOnly("IDX-1").getName());
    assertEquals("A2", index.getReadOnly("IDX-2").getName());
    assertEquals("A3", index.getReadOnly("IDX-3").getName());
    assertEquals(2, index.multiGetReadOnly(Arrays.asList("IDX-1", "IDX-2")).size());
    container.withLocalTx(() -> {
      assertEquals("A1", index.get("IDX-1").getName());
      assertEquals("A2", index.get("IDX-2").getName());
      assertEquals("A3", index.get("IDX-3").getName());
      assertEquals(2, index.multiGet(Arrays.asList("IDX-1", "IDX-2")).size());
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
      store.update("5", new TestObject("A5").setValue(5).setStrValue("IDX-4"));
    });
    JacisNonUniqueIndex<Object, String, TestObject> index = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1"));
      store.update("2", new TestObject("A2").setValue(5).setStrValue("IDX-2"));
    });
    assertEquals(1, index.getReadOnly("IDX-1").size());
    assertEquals(1, index.getReadOnly("IDX-2").size());
    assertEquals(1, index.getReadOnly("IDX-3").size());
    assertEquals(2, index.getReadOnly("IDX-4").size());
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

  @Test
  public void testUniqueIndexAddAndDelDuringTx() {
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
      store.update("3", new TestObject("A3").setValue(5).setStrValue("OTHER"));
      assertEquals(2, index.getReadOnly("IDX-0").size());
      store.update("3", new TestObject("A3").setValue(5).setStrValue("IDX-0"));
      assertEquals(3, index.getReadOnly("IDX-0").size());
    });
    assertEquals(3, index.getReadOnly("IDX-0").size());
  }

  @Test
  public void testNonUniqueIndexClearedDuringClear() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    JacisNonUniqueIndex<Object, String, TestObject> idx = store.createNonUniqueIndex("IDX-NAME", TestObject::getStrValue);
    assertEquals(0, idx.getReadOnly("IDX-1").size());
    container.withLocalTx(() -> store.update("1", new TestObject("A1").setValue(5).setStrValue("IDX-1")));
    assertEquals(1, idx.getReadOnly("IDX-1").size());
    container.withLocalTx(store::clear);
    assertEquals(0, idx.getReadOnly("IDX-1").size());
  }

}
