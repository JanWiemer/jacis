/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.testhelper;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.extension.objectadapter.cloning.microstream.JacisMicrostreamCloningObjectAdapter;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.plugin.objectadapter.serialization.JacisJavaSerializationObjectAdapter;
import org.jacis.plugin.objectadapter.serialization.JacisSerializationObjectAdapter;
import org.jacis.plugin.txadapter.local.JacisTransactionAdapterLocal;
import org.jacis.store.JacisStore;

import java.security.SecureRandom;

@SuppressWarnings("WeakerAccess")
public class JacisTestHelper {

  private TestTxAdapter testTxAdapter;

  public JacisContainer createTestContainer() {
    testTxAdapter = new TestTxAdapter();
    return new JacisContainer(testTxAdapter);
  }

  public JacisStore<String, TestObject> createTestStoreWithCloning() {
    JacisContainer container = createTestContainer();
    return createTestStoreWithCloning(container);
  }

  public JacisStore<String, TestObject> createTestStoreWithCloning(JacisContainer container) {
    JacisCloningObjectAdapter<TestObject> cloningAdapter = new JacisCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, cloningAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObject.class);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithCloningAndWithoutReadonlyMode() {
    JacisContainer container = createTestContainer();
    return createTestStoreWithCloningAndWithoutReadonlyMode(container);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithCloningAndWithoutReadonlyMode(JacisContainer container) {
    JacisCloningObjectAdapter<TestObjectWithoutReadOnlyMode> cloningAdapter = new JacisCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObjectWithoutReadOnlyMode, TestObjectWithoutReadOnlyMode> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObjectWithoutReadOnlyMode.class, cloningAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObjectWithoutReadOnlyMode.class);
  }

  public JacisStore<String, TestObject> createTestStoreWithMicrostreamCloning() {
    JacisContainer container = createTestContainer();
    return createTestStoreWithMicrostreamCloning(container);
  }

  public JacisStore<String, TestObject> createTestStoreWithMicrostreamCloning(JacisContainer container) {
    JacisMicrostreamCloningObjectAdapter<TestObject> cloningAdapter = new JacisMicrostreamCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, cloningAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObject.class);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithMicrostreamCloningAndWithoutReadonlyMode() {
    JacisContainer container = createTestContainer();
    return createTestStoreWithMicrostreamCloningAndWithoutReadonlyMode(container);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithMicrostreamCloningAndWithoutReadonlyMode(JacisContainer container) {
    JacisMicrostreamCloningObjectAdapter<TestObjectWithoutReadOnlyMode> cloningAdapter = new JacisMicrostreamCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObjectWithoutReadOnlyMode, TestObjectWithoutReadOnlyMode> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObjectWithoutReadOnlyMode.class, cloningAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObjectWithoutReadOnlyMode.class);
  }

  public JacisStore<String, TestObject> createTestStoreWithSerialization() {
    JacisContainer container = createTestContainer();
    return createTestStoreWithSerialization(container);
  }

  public JacisStore<String, TestObject> createTestStoreWithSerialization(JacisContainer container) {
    JacisSerializationObjectAdapter<TestObject> serializationAdapter = new JacisJavaSerializationObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObject, byte[]> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, serializationAdapter);
    return container.createStore(objectTypeSpec).getStore();
  }

  public JacisTransactionHandle suspendTx() {
    return testTxAdapter.suspendTx();
  }

  public void resumeTx(JacisTransactionHandle tx) {
    testTxAdapter.resumeTx(tx);
  }

  public long getRandBetween(long min, long max) {
    return max <= min ? min : min + new SecureRandom().nextLong(max - min + 1);
  }

  public void sleep(int ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static class TestTxAdapter extends JacisTransactionAdapterLocal {

    JacisTransactionHandle suspendTx() {
      JacisTransactionHandle currentTx = transaction.get();
      if (currentTx == null) {
        throw new JacisNoTransactionException("No active transaction to suspend!");
      }
      transaction.remove();
      return currentTx;
    }

    void resumeTx(JacisTransactionHandle tx) {
      JacisTransactionHandle currentTx = transaction.get();
      if (currentTx != null) {
        throw new JacisTransactionAlreadyStartedException("Failed to resume tx " + tx + " because another one is still active: " + currentTx);
      }
      transaction.set(tx);
    }

  }
}
