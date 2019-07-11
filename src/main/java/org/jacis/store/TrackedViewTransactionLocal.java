
package org.jacis.store;

import java.util.HashMap;
import java.util.Map;

import org.jacis.trackedviews.TrackedView;

/**
 * Wrapper class for a tracked view in order to keep track of the modifications done in the transaction in which the view snapshot has been taken.
 * This class is used by the JACIS store to track all modifications done
 * * inside this transaction up to the time when the snapshot is taken (in the method {@link JacisStoreTxView#getTrackedView(String, java.util.function.Supplier)})
 * * inside this transaction after the snapshot is taken (in the method {@link JacisStoreTxView#updateValue(StoreEntryTxView, Object)})
 * Therefore if you need the view multiple times inside a transaction (with some modifications between the accesses), you do not need to clone the view again.
 * Note that each update will track the modification from the previous update to this update.
 * 
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 */
class TrackedViewTransactionLocal<K, TV> {

  /** reference to the tracked view itself */
  final private TrackedView<TV> trackedView;
  /** map containing all objects modified during this transaction (containing always the last updated value) */
  final private Map<K, TV> lastUpdatedEntries = new HashMap<>();

  TrackedViewTransactionLocal(TrackedView<TV> trackedView) {
    this.trackedView = trackedView;
  }

  void trackModification(TV origValue, TV value, StoreEntryTxView<K, TV, ?> entry) {
    TV lastUpdatedValue = lastUpdatedEntries.get(entry.getKey());
    trackedView.trackModification(lastUpdatedValue == null ? origValue : lastUpdatedValue, value);
    @SuppressWarnings("unchecked")
    TV clone = (TV) entry.getCommittedEntry().getStore().getObjectAdapter().cloneTxView2Committed(value);
    lastUpdatedEntries.put(entry.getKey(), clone);
  }

  TrackedView<TV> getTrackedView() {
    return trackedView;
  }
}
