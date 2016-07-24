/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.serialization;

import org.jacis.exception.JacisStaleObjectException;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class JacisStoreWithSerializationMultithreadedTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreWithSerializationMultithreadedTest.class);

  @Test()
  public void testSimpleMultiThreadedAccess() {
    for (int iter = 0; iter < 10; iter++) {
      checkSimpleMultiThreadedAccess();
    }
  }

  public void checkSimpleMultiThreadedAccess() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    String testObjName = "TST";
    store.getContainer().withLocalTx(() -> {
      store.update(testObjName, new TestObject(testObjName, 0).setStrValue("0"));
    });
    int numberOfThreads = 5;
    Thread[] threads = new Thread[numberOfThreads];
    for (int i = 0; i < numberOfThreads; i++) {
      final int id = i;
      threads[i] = new Thread("TestThread-" + i) {
        @Override
        public void run() {
          try {
            AtomicBoolean finished = new AtomicBoolean(false);
            while (!finished.get()) {
              try {
                store.getContainer().withLocalTx(() -> {
                  TestObject obj = store.get(testObjName);
                  log.debug("{} reads {}", Thread.currentThread().getName(), obj);
                  if (obj.getStrValue().endsWith("" + id)) {
                    obj.setStrValue(obj.getStrValue() + (id + 1));
                    log.info("{} found its trigger value -> new content: {} ", Thread.currentThread().getName(), obj);
                    store.update(testObjName, obj);
                    finished.set(true);
                  }
                });
                Thread.sleep(10);
              } catch (JacisStaleObjectException e) {
                log.info("caught stale object exception: {}", e, e);
              }
            }
          } catch (InterruptedException e) {
            log.info("interupted thread {}: {}", Thread.currentThread().getName(), e, e);
            Thread.currentThread().interrupt();
          } catch (Throwable t) {
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
      } catch (InterruptedException e) {
        log.info("join interrupted for thread {}: {]", threads[i].getName(), e);
      }
    }
    store.getContainer().withLocalTx(() -> {
      TestObject obj = store.get(testObjName);
      log.info("Result:" + obj);
      assertEquals("012345", obj.getStrValue());
    });
  }

}
