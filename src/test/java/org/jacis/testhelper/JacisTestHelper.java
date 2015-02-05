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

public class JacisTestHelper {

  private TestTxAdapter testTxAdapter;

  public JacisStore<String, TestObject, TestObject> createTestStoreWithCloning() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
    return createTestStoreWithCloning(container);
  }

  public JacisStore<String, TestObject, TestObject> createTestStoreWithCloning(JacisContainer container) {
    JacisCloningObjectAdapter<TestObject> serializationAdapter = new JacisCloningObjectAdapter<TestObject>();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, serializationAdapter);
    container.createStore(objectTypeSpec);
    JacisStore<String, TestObject, TestObject> store = container.getStore(String.class, TestObject.class);
    return store;
  }

  public JacisStore<String, TestObject, byte[]> createTestStoreWithSerialization() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
    return createTestStoreWithSerialization(container);
  }

  public JacisStore<String, TestObject, byte[]> createTestStoreWithSerialization(JacisContainer container) {
    JacisSerializationObjectAdapter<TestObject> serializationAdapter = new JacisJavaSerializationObjectAdapter<TestObject>();
    JacisObjectTypeSpec<String, TestObject, byte[]> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, serializationAdapter);
    return container.createStore(objectTypeSpec);
  }

  public JacisTransactionHandle suspendTx() {
    return testTxAdapter.suspendTx();
  }

  public void resumeTx(JacisTransactionHandle tx) {
    testTxAdapter.resumeTx(tx);
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
}
