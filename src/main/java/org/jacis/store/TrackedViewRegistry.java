/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.JacisTransactionListener;
import org.jacis.plugin.JacisTransactionListenerAdapter;
import org.jacis.trackedviews.TrackedView;
import org.jacis.trackedviews.TrackedViewClustered;

import java.util.*;

/**
 * Registry where tracked views can be registered for an object store.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Value type of the store entry
 * @author Jan Wiemer
 */
@SuppressWarnings("unused")
public class TrackedViewRegistry<K, TV> implements JacisModificationListener<K, TV> {

  /** Reference to the JACIS store the tracked view registry belongs to */
  private final JacisStoreImpl<K, TV, ?> store;
  /** Map assigning the tracked views maintained by this registry to the view classes */
  private final Map<Class<? extends TrackedView<TV>>, TrackedView<TV>> viewMap = new HashMap<>();

  TrackedViewRegistry(JacisStoreImpl<K, TV, ?> store, boolean checkConsistencyAfterCommit) {
    this.store = store;
    if (checkConsistencyAfterCommit) {
      JacisTransactionListener txListener = new JacisTransactionListenerAdapter() {

        @Override
        public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
          checkTrackedViewsAfterCommit();
        }

      };
      store.getContainer().registerTransactionListener(txListener);
    }

  }

  @SuppressWarnings("unchecked")
  public void registerTrackedView(TrackedView<TV> view) {
    if (!store.getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering tracked views is only possible if the store is keeping track of the original values of a transactional view.");
    }
    store.executeAtomic(() -> initTrackedView(view));
    viewMap.put((Class<? extends TrackedView<TV>>) view.getClass(), view);
  }

  public Collection<TrackedView<TV>> getAllViews() {
    return viewMap.values();
  }

  public Set<Class<? extends TrackedView<TV>>> getAllViewClasses() {
    return viewMap.keySet();
  }

  public boolean containsView(Class<? extends TrackedView<TV>> viewType) {
    return viewMap.containsKey(viewType);
  }

  @Override
  public void onModification(K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    for (TrackedView<TV> view : viewMap.values()) {
      view.trackModification(oldValue, newValue);
    }
  }

  private void checkTrackedViewsAfterCommit() {
    List<TV> values = store.getAllReadOnly(null);
    for (TrackedView<TV> view : viewMap.values()) {
      view.checkView(values);
    }
  }

  void clearViews() {
    viewMap.values().forEach(TrackedView::clear);
  }

  public <VT extends TrackedView<TV>> VT getView(Class<VT> viewType) {
    VT view = store.computeAtomic(() -> getAndCloneView(viewType));
    JacisStoreTxView<K, TV, ?> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, TV, ?> entryTxView : txView.getAllEntryTxViews()) {
        view.trackModification(entryTxView.getOrigValue(), entryTxView.getValue());
      }
    }
    return view;
  }

  @SuppressWarnings("unchecked")
  public <SVK> Collection<SVK> getSubViewKeys(Class<? extends TrackedViewClustered<TV, SVK, ? extends TrackedView<TV>>> viewType) {
    return store.computeAtomic(() -> {
      TrackedView<TV> view = viewMap.get(viewType);
      if (view == null) {
        throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
      } else if (!viewType.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type! view: " + view);
      } else if (!TrackedViewClustered.class.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of " + TrackedViewClustered.class + "! view: " + view);
      }
      TrackedViewClustered<TV, SVK, ? extends TrackedView<TV>> clusteredView = (TrackedViewClustered<TV, SVK, ? extends TrackedView<TV>>) view;
      return clusteredView.getSubViewKeys();
    });
  }

  @SuppressWarnings("unchecked")
  public <VT extends TrackedView<TV>, VK> VT getSubView(Class<? extends TrackedViewClustered<TV, VK, VT>> viewType, VK subviewKey) {
    VT subViewClone = store.computeAtomic(() -> {
      TrackedView<TV> view = viewMap.get(viewType);
      if (view == null) {
        throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
      } else if (!viewType.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type! view: " + view);
      } else if (!TrackedViewClustered.class.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of " + TrackedViewClustered.class + "! view: " + view);
      }
      TrackedViewClustered<TV, VK, TrackedView<TV>> clusteredView = (TrackedViewClustered<TV, VK, TrackedView<TV>>) view;
      TrackedView<TV> subView = Objects.requireNonNull(clusteredView.getSubView(subviewKey), "No sub-view found for key " + subviewKey);
      return (VT) subView.clone();
    });
    JacisStoreTxView<K, TV, ?> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, TV, ?> entryTxView : txView.getAllEntryTxViews()) {
        subViewClone.trackModification(entryTxView.getOrigValue(), entryTxView.getValue());
      }
    }
    return subViewClone;
  }

  @SuppressWarnings("unchecked")
  private <VT extends TrackedView<TV>> VT getAndCloneView(Class<? extends TrackedView<TV>> viewType) {
    VT view = (VT) viewMap.get(viewType);
    if (view == null) {
      throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
    } else if (!viewType.isInstance(view)) {
      throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type: " + view);
    }
    return (VT) view.clone();
  }

  public <VT extends TrackedView<TV>> void reinitializeView(Class<VT> viewType) {
    store.executeAtomic(() -> initTrackedView(getView(viewType)));
  }

  private void initTrackedView(TrackedView<TV> view) {
    view.clear();
    for (TV val : store.getAllReadOnly(null)) {
      view.trackModification(null, val);
    }
  }

}
