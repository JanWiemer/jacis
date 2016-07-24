/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.cloning;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.JacisTransactionListenerAdapter;
import org.jacis.store.JacisStore;
import org.jacis.store.TrackedViewRegistry;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.jacis.testhelper.TrackedTestView;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JacisStoreWithCloningAndTrackedViewMultithreadedTest {

  static final Logger log = LoggerFactory.getLogger(JacisStoreWithCloningAndTrackedViewMultithreadedTest.class);

  @Test()
  public void testMultiThreadedAccess() {
    JacisTestHelper testHelper = new JacisTestHelper();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    final JacisStore<String, TestObject, TestObject> store = testHelper.createTestStoreWithCloning();
    final JacisContainer container = store.getContainer();
    final TestJacisTransactionListenerAdapter txListener = new TestJacisTransactionListenerAdapter(store);
    container.registerTransactionListener(txListener);
    store.getTrackedViewRegistry().registerTrackedView(new TrackedTestView());
    int numberOfThreads = 10;
    int numberOfObjects = 50;
    Thread[] threads = new Thread[numberOfThreads];
    long deadline = System.currentTimeMillis() + 5 * 1000;
    for (int i = 0; i < numberOfThreads; i++) {
      threads[i] = new Thread("TestThread" + i) {
        @Override
        public void run() {
          try {
            int iter = 0;
            while (System.currentTimeMillis() < deadline && exception.get() == null) {
              int currIter = ++iter;
              container.withLocalTxAndRetry(20, () -> {
                long objId = testHelper.getRandBetween(1, numberOfObjects);
                long inc = testHelper.getRandBetween(0, 10);
                String objName = "Obj" + objId;
                TestObject object = store.get(objName);
                String operation = object == null ? "(new)" : "(existing)";
                if (object == null) {
                  object = new TestObject(objName, 0);
                }
                long oldVal = object.getValue();
                long totalInc;
                if (inc == 0) {
                  log.debug("{} iter {} remove {} {} (oldVal: {}) --> {}", getName(), currIter, operation, object, oldVal, Thread.currentThread().getName());
                  store.remove(objName);
                  totalInc = -object.getValue();
                } else {
                  log.debug("{} iter {} inc {} {} by {} from {} to {} --> {}", getName(), currIter, operation, object, inc, oldVal, oldVal + inc, Thread.currentThread().getName());
                  object.setValue(oldVal + (int) inc);
                  store.update(objName, object);
                  totalInc = (int) inc;
                }
                if (inc < 5) {
                  TrackedViewRegistry<String, TestObject, TestObject> tvr = store.getTrackedViewRegistry();
                  log.debug("... read current view values: count={}, sum={}", tvr.getView(TrackedTestView.class).getCount(), tvr.getView(TrackedTestView.class).getSum());
                }
                String updateTxt = object.getName() + " form " + oldVal + " to " + (oldVal + inc);
                txListener.trackTx(totalInc, updateTxt);
              });
            }
          } catch (Throwable t) {
            exception.set(t);
            log.error("Unexpected Exception {}", t, t);
          } finally {
            log.debug("terminate {}", Thread.currentThread().getName());
          }
        }
      };
    }
    for (int i = 0; i < numberOfThreads; i++) {
      threads[i].start();
    }
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      log.info("sleep interrupted {}", e, e);
    }
    for (int i = 0; i < numberOfThreads; i++) {
      try {
        threads[i].join();
        log.info("Thread finished: {}", threads[i].getName());
      } catch (InterruptedException e) {
        log.info("join interrupted for thread {}: {}", threads[i].getName(), e);
      }
    }
    TrackedTestView view = store.getTrackedViewRegistry().getView(TrackedTestView.class);
    store.streamReadOnly().forEach(v -> log.info(" core value: {}", v));
    log.info("coreCount=" + store.streamReadOnly().filter(v -> v != null).count());
    log.info("viewCount=" + view.getCount());
    long viewSum = view.getSum();
    long storeSum = store.streamReadOnly().mapToLong(v -> v == null ? 0 : v.getValue()).sum();
    long expectedSum = txListener.getSum();
    log.info("expectedSum = {}", expectedSum);
    log.info("storeSum    = {}", storeSum);
    log.info("viewSum     = {}", viewSum);
    testHelper.sleep(1000);
    log.info("expectedSumAfterAWhile = {}", txListener.getSum());
    assertEquals(expectedSum, viewSum);
    assertTrue("more than " + numberOfObjects + ": " + view.getCount() + "!", view.getCount() <= numberOfObjects);
    Throwable e = exception.get();
    if (e != null) {
      throw new RuntimeException("Exception occured in one thread: " + e, e);
    }
  }

  public static class TestViewValue {
    private final int count;
    private final int sum;

    public TestViewValue(int count, int sum) {
      this.count = count;
      this.sum = sum;
    }

    public int getCount() {
      return count;
    }

    public int getSum() {
      return sum;
    }

  }

  public static class TestJacisTransactionListenerAdapter extends JacisTransactionListenerAdapter {

    private final AtomicLong sum = new AtomicLong(0l);
    private JacisStore<String, TestObject, TestObject> store;
    private ThreadLocal<Long> totalInc = new ThreadLocal<>();
    private ThreadLocal<String> updateTxt = new ThreadLocal<>();

    public TestJacisTransactionListenerAdapter(JacisStore<String, TestObject, TestObject> store) {
      this.store = store;
    }

    public void trackTx(long totalInc, String updateTxt) {
      this.totalInc.set(totalInc);
      this.updateTxt.set(updateTxt);
    }

    public long getSum() {
      return sum.get();
    }

    @Override
    public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
      super.afterCommit(container, tx);
      Long ti = totalInc.get();
      long newVal = sum.getAndAdd(ti);
      log.debug("...AFTER COMMIT: incremented object {} Thread: {}", updateTxt.get(), Thread.currentThread().getName());
      log.debug("...AFTER COMMIT: incremented expected sum by {} -> new Value {}  Thread: {}", ti, newVal + ti, Thread.currentThread().getName());
      long viewSum = store.getTrackedViewRegistry().getView(TrackedTestView.class).getSum();
      long expectedSum = sum.get();
      if (viewSum != expectedSum) {
        log.warn("!!! View sum differs! View sum = " + viewSum + " expected = " + expectedSum + " Thread: " + Thread.currentThread().getName());
        log.info("!!! count=" + store.getTrackedViewRegistry().getView(TrackedTestView.class).getCount());
        long storeSum = store.streamReadOnly().mapToLong(v -> v == null ? null : v.getValue()).sum();
        log.info("!!! storeSum    = {}", storeSum);
        log.info("!!! viewSum     = {}", viewSum);
        log.info("!!! expectedSum = {} (now: {})", expectedSum, sum.get());
        store.streamReadOnly().forEach(v -> log.info(" core value: {}", v));
        throw new IllegalStateException("View sum differs! View sum = " + viewSum + " expected = " + expectedSum);
      }
      this.totalInc.set(0l);
      this.updateTxt.set("-");
    }

    @Override
    public void afterRollback(JacisContainer container, JacisTransactionHandle tx) {
      super.afterRollback(container, tx);
      this.totalInc.set(0l);
      this.updateTxt.set("-");
    }

  }

}
