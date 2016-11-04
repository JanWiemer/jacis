/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.testhelper;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.plugin.objectadapter.serialization.JacisJavaSerializationObjectAdapter;
import org.jacis.plugin.objectadapter.serialization.JacisSerializationObjectAdapter;
import org.jacis.plugin.txadapter.local.JacisTransactionAdapterLocal;
import org.jacis.store.JacisStore;

@SuppressWarnings("WeakerAccess")
public class JacisTestHelper {

  private TestTxAdapter testTxAdapter;

  public JacisStore<String, TestObject> createTestStoreWithCloning() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
    return createTestStoreWithCloning(container);
  }

  public JacisStore<String, TestObject> createTestStoreWithCloning(JacisContainer container) {
    JacisCloningObjectAdapter<TestObject> serializationAdapter = new JacisCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, serializationAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObject.class);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithCloningAndWithoutReadonlyMode() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
    return createTestStoreWithCloningAndWithoutReadonlyMode(container);
  }

  public JacisStore<String, TestObjectWithoutReadOnlyMode> createTestStoreWithCloningAndWithoutReadonlyMode(JacisContainer container) {
    JacisCloningObjectAdapter<TestObjectWithoutReadOnlyMode> serializationAdapter = new JacisCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObjectWithoutReadOnlyMode, TestObjectWithoutReadOnlyMode> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObjectWithoutReadOnlyMode.class, serializationAdapter);
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObjectWithoutReadOnlyMode.class);
  }

  public JacisStore<String, TestObject> createTestStoreWithSerialization() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
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
    return max <= min ? min : min + Math.round((max - min) * Math.random());
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
