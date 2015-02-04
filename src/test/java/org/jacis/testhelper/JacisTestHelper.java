package org.jacis.testhelper;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.plugin.txadapter.JacisTransactionAdapterLocal;
import org.jacis.store.JacisStore;

public class JacisTestHelper {

  private TestTxAdapter testTxAdapter;

  public JacisStore<String, TestObject, TestObject> createTestStore() {
    testTxAdapter = new TestTxAdapter();
    JacisContainer container = new JacisContainer(testTxAdapter);
    return createTestStore(container);
  }

  public JacisStore<String, TestObject, TestObject> createTestStore(JacisContainer container) {
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, new JacisCloningObjectAdapter<TestObject>());
    container.createStore(objectTypeSpec);
    JacisStore<String, TestObject, TestObject> store = container.getStore(String.class, TestObject.class);
    return store;
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
