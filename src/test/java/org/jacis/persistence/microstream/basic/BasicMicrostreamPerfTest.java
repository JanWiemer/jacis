/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.persistence.microstream.basic;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jacis.testhelper.FileUtils;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.jdk8.java.util.BinaryHandlersJDK8;
import one.microstream.storage.configuration.Configuration;
import one.microstream.storage.types.EmbeddedStorageFoundation;
import one.microstream.storage.types.EmbeddedStorageManager;

public class BasicMicrostreamPerfTest {

  private static final Logger log = LoggerFactory.getLogger(BasicMicrostreamPerfTest.class);

  protected static Path getStorageDir(String suffix) {
    Path path = suffix == null ? Paths.get("var", BasicMicrostreamPerfTest.class.getName()) : Paths.get("var", BasicMicrostreamPerfTest.class.getName(), suffix);
    log.debug("use storage path: {}", path.toString());
    return path;
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteDirectory(Paths.get("var", BasicMicrostreamPerfTest.class.getName()).toFile());
  }

  protected EmbeddedStorageManager createStorageManager(Path storageDir) {
    EmbeddedStorageFoundation<?> foundation = Configuration.Default() //
        .setBaseDirectory(storageDir.toString()) //
        .createEmbeddedStorageFoundation();
    foundation.onConnectionFoundation(BinaryHandlersJDK8::registerJDK8TypeHandlers);
    EmbeddedStorageManager storageManager = foundation.createEmbeddedStorageManager().start();
    return storageManager;
  }

  @Test
  @Ignore
  public void testPerformance() {
    log.info("===== TEST BULK UPDATE PERFORMANCE =====");
    Arrays.asList(1_000, 10_000, 100_000, 1_000_000, 2_000_000).forEach(size -> {
      doTestBulkUpdatePerfArray(size, 100);
      doTestBulkUpdatePerfArrayList(size, 100);
      doTestBulkUpdatePerfHashMap(size, 100);
      log.info("----------------------------------------");
    });
    log.info("===== TEST UPDATE PERFORMANCE =====");
    Arrays.asList(1_000, 10_000, 100_000, 1_000_000, 2_000_000).forEach(size -> {
      doTestUpdatePerfArray(size, 100);
      doTestUpdatePerfArrayList(size, 100);
      doTestUpdatePerfLinkedList(size,100);
      doTestUpdatePerfHashMap(size, 100);
      log.info("----------------------------------------");
    });
    log.info("===== TEST INSERT PERFORMANCE =====");
    Arrays.asList(1_000, 10_000, 20_000, 30_000).forEach(n -> {
      doTestInsertPerfArrayList(n);
      doTestInsertPerfLinkedList(n);
      doTestInsertPerfHashMap(n);
      log.info("----------------------------------------");
    });
  }

  protected void doTestUpdatePerfArray(int size, int n) {
    Path storageDir = getStorageDir("doTestUpdatePerfArray-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    String[] data = new String[size];
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data[i] = "Element " + i;
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data[idx] = "Element New " + idx;
      storageManager.storeRoot();
    }
    log.info("updating {} / {} Array entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestUpdatePerfArrayList(int size, int n) {
    Path storageDir = getStorageDir("doTestUpdatePerfArrayList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    List<String> data = new ArrayList<>(size);
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data.add("Element " + i);
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data.set(idx, "Element New " + idx);
      storageManager.storeRoot();
    }
    log.info("updating {} / {} ArrayList entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestUpdatePerfLinkedList(int size, int n) {
    Path storageDir = getStorageDir("doTestUpdatePerfLinkedList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    LinkedList<String> data = new LinkedList<>();
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data.add("Element " + i);
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    ListIterator<String> it = data.listIterator();
    for (int i = 0; i < n; i++) {
      it.next();
      it.set("Element New " + i);
      storageManager.storeRoot();
    }
    log.info("updating {} / {} LinkedList entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestUpdatePerfHashMap(int size, int n) {
    Path storageDir = getStorageDir("doTestUpdatePerfHashMap-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    Map<Integer, String> data = new HashMap<>();
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data.put(i, "Element " + i);
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data.put(idx, "Element New " + idx);
      storageManager.storeRoot();
    }
    log.info("updating {} / {} HashMap entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestBulkUpdatePerfArray(int size, int n) {
    Path storageDir = getStorageDir("doTestBulkUpdatePerfArray-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    String[] data = new String[size];
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data[i] = "Element " + i;
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data[idx] = "Element New " + idx;
    }
    storageManager.storeRoot();
    log.info("bulk updating {} / {} Array entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestBulkUpdatePerfArrayList(int size, int n) {
    Path storageDir = getStorageDir("doTestBulkUpdatePerfArrayList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    List<String> data = new ArrayList<>(size);
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data.add("Element " + i);
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data.set(idx, "Element New " + idx);
    }
    storageManager.storeRoot();
    log.info("bulk updating {} / {} ArrayList entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestBulkUpdatePerfHashMap(int size, int n) {
    Path storageDir = getStorageDir("doTestBulkUpdatePerfHashMap-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    Map<Integer, String> data = new HashMap<>();
    storageManager.setRoot(data);
    // long t0 = System.nanoTime();
    for (int i = 0; i < size; i++) {
      data.put(i, "Element " + i);
    }
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      int idx = 2 * i % n;
      data.put(idx, "Element New " + idx);
      storageManager.storeRoot();
    }
    log.info("bulk updating {} / {} HashMap entries took {}", n, size, stopTime(t1, n));
    storageManager.shutdown();
  }

  protected void doTestInsertPerfArrayList(int n) {
    Path storageDir = getStorageDir("doTestInsertPerfArrayList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    List<String> data = new ArrayList<>();
    storageManager.setRoot(data);
    long t0 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      data.add("Element " + i);
      storageManager.storeRoot();
    }
    log.info("storing {} ArrayList adds took {}", n, stopTime(t0, n));
    storageManager.shutdown();
  }
  
  protected void doTestInsertPerfLinkedList(int n) {
    Path storageDir = getStorageDir("doTestInsertPerfLinkedList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    List<String> data = new LinkedList<>();
    storageManager.setRoot(data);
    long t0 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      data.add("Element " + i);
      storageManager.storeRoot();
    }
    log.info("storing {} LinkedList adds took {}", n, stopTime(t0, n));
    storageManager.shutdown();
  }

  protected void doTestInsertPerfHashMap(int n) {
    Path storageDir = getStorageDir("doTestInsertPerfArrayList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    Map<Integer, String> data = new HashMap<>();
    storageManager.setRoot(data);
    long t0 = System.nanoTime();
    for (int i = 0; i < n; i++) {
      data.put(i, "Element " + i);
      storageManager.storeRoot();
    }
    log.info("storing {} HashMap adds took {}", n, stopTime(t0, n));
    storageManager.shutdown();
  }

  protected String stopTime(long startTimeNs, Integer operations) {
    long nanos = System.nanoTime() - startTimeNs;
    double millis = ((double) nanos) / (1000 * 1000);
    if (operations == null) {
      return String.format("%.2f ms", millis);
    } else {
      return String.format("%.2f ms (%.2f ms/operation)", millis, millis / operations);
    }
  }

}
