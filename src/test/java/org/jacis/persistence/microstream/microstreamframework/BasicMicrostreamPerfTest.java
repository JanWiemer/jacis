/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.persistence.microstream.microstreamframework;

import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.jacis.testhelper.FileUtils;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Ignore
public class BasicMicrostreamPerfTest {

  private static final int MAX_TEST_SIZE = 500;
//  private static final int MAX_TEST_SIZE = Integer.MAX_VALUE;

  private static final Logger log = LoggerFactory.getLogger(BasicMicrostreamPerfTest.class);

  protected static Path getStorageDir(String suffix) {
    Path path = suffix == null ? Paths.get("var", BasicMicrostreamPerfTest.class.getName()) : Paths.get("var", BasicMicrostreamPerfTest.class.getName(), suffix);
    log.debug("use storage path: {}", path);
    return path;
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteDirectory(Paths.get("var", BasicMicrostreamPerfTest.class.getName()).toFile());
  }

  protected EmbeddedStorageManager createStorageManager(Path storageDir) {
    EmbeddedStorageFoundation<?> foundation = EmbeddedStorageConfigurationBuilder.New() //
        .setStorageDirectory(storageDir.toString()) //
        .setBackupDirectory(storageDir + "/backup") //
        .setDeletionDirectory(storageDir + "/deletion") //
        .createEmbeddedStorageFoundation();
    return foundation.createEmbeddedStorageManager().start();
  }

  @Test
  @Ignore
  public void testBulkUpdatePerformance() {
    log.info("===== TEST BULK UPDATE PERFORMANCE =====");
    Arrays.asList(200, 500, 1_000, 10_000, 100_000, 1_000_000, 2_000_000).stream().filter(size -> size <= MAX_TEST_SIZE).forEach(size -> {

      doTestBulkUpdatePerfArray(size, 100);
      doTestBulkUpdatePerfArrayList(size, 100);
      doTestBulkUpdatePerfHashMap(size, 100);
      log.info("----------------------------------------");
    });
  }

  @Test
  @Ignore
  public void testUpdatePerformance() {
    log.info("===== TEST UPDATE PERFORMANCE =====");
    Arrays.asList(200, 500, 1_000, 10_000, 100_000, 1_000_000, 2_000_000).stream().filter(size -> size <= MAX_TEST_SIZE).forEach(size -> {
      doTestUpdatePerfArray(size, 100);
      doTestUpdatePerfArrayList(size, 100);
      doTestUpdatePerfLinkedList(size, 100);
      doTestUpdatePerfCustomLinkedList(size, 100);
      doTestUpdatePerfHashMap(size, 100);
      log.info("----------------------------------------");
    });
  }

  @Test
  @Ignore
  public void testInsertPerformance() {
    log.info("===== TEST INSERT PERFORMANCE =====");
    Arrays.asList(200, 500, 1_000, 10_000, 20_000, 30_000).stream().filter(size -> size <= MAX_TEST_SIZE).forEach(n -> {
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

  protected void doTestUpdatePerfCustomLinkedList(int size, int n) {
    Path storageDir = getStorageDir("doTestUpdatePerfCustomLinkedList-" + n);
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    ListElement head = new ListElement("Element " + 0);
    ListElement prv = head;
    // long t0 = System.nanoTime();
    for (int i = 1; i < size; i++) {
      ListElement entry = new ListElement("Element " + i).setPrv(prv);
      prv.setNxt(entry);
      prv = entry;
    }
    storageManager.setRoot(head);
    storageManager.storeRoot();
    long t1 = System.nanoTime();
    ListElement it = head;
    for (int i = 0; i < n; i++) {
      it = it.nxt;
      storageManager.store(it.setValue("Element New " + i));
      it = it.nxt;
    }
    log.info("updating {} / {} Custom LinkedList entries took {}", n, size, stopTime(t1, n));
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

class ListElement {
  String value;
  ListElement nxt;
  ListElement prv;

  public ListElement(String value) {
    this.value = value;
  }

  public ListElement setValue(String value) {
    this.value = value;
    return this;
  }

  public ListElement setNxt(ListElement nxt) {
    this.nxt = nxt;
    return this;
  }

  public ListElement setPrv(ListElement prv) {
    this.prv = prv;
    return this;
  }

}
