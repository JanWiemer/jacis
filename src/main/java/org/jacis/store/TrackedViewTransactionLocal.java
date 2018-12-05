package org.jacis.store;

import java.util.HashMap;
import java.util.Map;

import org.jacis.trackedviews.TrackedView;

class TrackedViewTransactionLocal<TV, K> {
  final private TrackedView<TV> trackedView;
  final private Map<StoreEntryTxView<K, TV, ?>, TV> lastEntries = new HashMap<>();

  TrackedViewTransactionLocal(TrackedView<TV> trackedView) {
    this.trackedView = trackedView;
  }

  void trackModification(TV origValue, TV value, StoreEntryTxView<K, TV, ?> entry) {
    TV lastUpdatedValue = lastEntries.get(entry);
    trackedView.trackModification(lastUpdatedValue == null ? origValue : lastUpdatedValue, value);
    @SuppressWarnings("unchecked")
    TV clone = (TV) entry.getCommittedEntry().getStore().getObjectAdapter().cloneTxView2Committed(value);
    lastEntries.put(entry, clone);
  }

  TrackedView<TV> getTrackedView() {
    return trackedView;
  }
}
