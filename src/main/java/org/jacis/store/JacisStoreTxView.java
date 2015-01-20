package org.jacis.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jacis.container.JacisTransactionHandle;

/**
 * @author Jan Wiemer
 * 
 * Representing a committed version of an entry in the store.
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
class JacisStoreTxView<K, V> {

  private JacisStore<K, V> store; // main store
  private final JacisTransactionHandle tx; // the transaction 
  private final long creationTimestamp; // in system milliseconds (timestamp usually set at first access returning a TX view)
  private final Map<K, StoreEntryTxView<K, V>> storeTxView; // entries with an own view in this TX
  private boolean commitPending = false; // commit pending / prepare already called
  private final String readOnlyTxName; // the name of the TX if this is a read only snapshot (null <-> writable)

  public JacisStoreTxView(JacisStore<K, V> store, JacisTransactionHandle transaction) {
    this.store = store;
    this.tx = transaction;
    this.readOnlyTxName = null;
    this.creationTimestamp = System.currentTimeMillis();
    this.storeTxView = new HashMap<>();
  }

  public JacisStoreTxView(String readOnlyTxName, JacisStoreTxView<K, V> orig) { // only to create a read only snapshot
    this.tx = orig.tx;
    this.readOnlyTxName = readOnlyTxName;
    this.creationTimestamp = orig.creationTimestamp;
    Map<K, StoreEntryTxView<K, V>> origCache = orig.storeTxView;
    Map<K, StoreEntryTxView<K, V>> readOnlyCache = new HashMap<>(origCache.size());
    for (Entry<K, StoreEntryTxView<K, V>> mapEntry : origCache.entrySet()) {
      StoreEntryTxView<K, V> cacheEntry = new StoreEntryTxView<>(mapEntry.getValue());
      readOnlyCache.put(mapEntry.getKey(), cacheEntry);
    }
    storeTxView = readOnlyCache;
  }

  public String getTxName() {
    return readOnlyTxName == null ? tx.getTxName() : readOnlyTxName + "|" + tx.getTxName();
  }

  public JacisTransactionHandle getTransaction() {
    return tx;
  }

  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  public boolean isReadOnly() {
    return readOnlyTxName != null;
  }

  public boolean isCommitPending() {
    return commitPending;
  }

  public JacisStoreTxView<K, V> assertWritable() {
    if (commitPending) {
      throw new IllegalStateException("Commit already started for transaction " + this);
    } else if (isReadOnly()) {
      throw new IllegalStateException("No changes allowed for read only transaction " + this);
    }
    return this;
  }

  public boolean containsTxView(K key) {
    return storeTxView.containsKey(key);
  }

  public StoreEntryTxView<K, V> getEntryTxView(K key) {
    return storeTxView.get(key);
  }

  public Collection<StoreEntryTxView<K, V>> getAllEntryTxViews() {
    return storeTxView.values();
  }

  public StoreEntryTxView<K, V> createTxViewEntry(StoreEntry<K, V> commitedEntry) {
    StoreEntryTxView<K, V> entry = new StoreEntryTxView<>(commitedEntry, store.getObjectTypeSpec().isTrackOriginalValueEnabled());
    storeTxView.put(entry.getKey(), entry);
    return entry;
  }

  public boolean removeTxViewEntry(K key, boolean forceIfUpdated) {
    StoreEntryTxView<K, V> entry = storeTxView.get(key);
    if (entry.isUpdated() && !forceIfUpdated) {
      return false;
    }
    storeTxView.remove(key);
    return true;
  }

  public void startCommitPhase() {
    this.commitPending = true;
  }

  public void destroy() {
    storeTxView.clear();
    store.notifyTxViewDestroyed(this);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    if (readOnlyTxName != null) {
      b.append(readOnlyTxName).append("(snapshot-from:").append(tx).append(")");
    } else {
      b.append(tx);
    }
    if (commitPending) {
      b.append("[COMMIT-PENDING]");
    }
    b.append("(#entries=").append(storeTxView.size()).append(")");
    if (isReadOnly()) {
      b.append("(readOnly)");
    }
    return b.toString();
  }

}
