/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.performance;

import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacisPerformanceTest {

  private static final Logger log = LoggerFactory.getLogger(JacisPerformanceTest.class);

  @Test
  public void testInsertPerformanceSerialization() {
    JacisStore<String, TestObject, ?> store = new JacisTestHelper().createTestStoreWithSerialization();
    long mem0 = getUsedMem();
    long t0 = System.nanoTime();
    int nTx = 10000;
    int nIns = 10;
    doInserts(store, nTx, nIns);
    long duration = System.nanoTime() - t0;
    long mem = getUsedMem() - mem0;
    log.info("Jacis Store based on Serialization {} TX a {} inserts: {} ms {} bytes", nTx, nIns, milliStr(duration), memStr(mem));
  }

  @Test
  public void testInsertPerformanceCloning() {
    JacisStore<String, TestObject, ?> store = new JacisTestHelper().createTestStoreWithCloning();
    long mem0 = getUsedMem();
    long t0 = System.nanoTime();
    int nTx = 10000;
    int nIns = 10;
    doInserts(store, nTx, nIns);
    long duration = System.nanoTime() - t0;
    long mem = getUsedMem() - mem0;
    log.info("Jacis Store based on cloning       {} TX a {} inserts: {} ms {} bytes", nTx, nIns, milliStr(duration), memStr(mem));
  }

  protected void doInserts(JacisStore<String, TestObject, ?> store, int nTx, int sizeTx) {
    for (int i = 0; i < nTx; i++) {
      int txIdx = i;
      store.getContainer().withLocalTx(() -> {
        for (int j = 0; j < sizeTx; j++) {
          TestObject testObject = new TestObject("obj-" + txIdx + "-" + j, txIdx + j);
          store.update(testObject.getName(), testObject);
        }
      });
    }
  }

  private long getUsedMem() {
    for (int i = 0; i < 10; i++) {
      Runtime.getRuntime().gc();
    }
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public String milliStr(long nanos) {
    return String.format("%.2f", ((double) nanos) / (double) (1000 * 1000 * 1000));
  }

  public String memStr(long bytes) {
    boolean si = true;
    int unit = si ? 1000 : 1024;
    if (bytes < unit) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

}
