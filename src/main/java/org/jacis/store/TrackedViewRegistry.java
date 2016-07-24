/*
 * Copyright (c) 2016. Jan Wiemer
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
 * @author Jan Wiemer
 * 
 * Registry where tracked views can be registered for an object store.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Value type of the store entry
 */
public class TrackedViewRegistry<K, TV, CV> implements JacisModificationListener<K, TV> {

  private final JacisStore<K, TV, CV> store;
  private final Map<Class<? extends TrackedView<TV>>, TrackedView<TV>> viewMap = new HashMap<>();
  private final JacisTransactionListener txListener = new JacisTransactionListenerAdapter() {

    @Override
    public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
      checkTrackedViewsAfterCommit();
    }

  };

  public TrackedViewRegistry(JacisStore<K, TV, CV> store, boolean checkConsistencAfterCommit) {
    this.store = store;
    if (checkConsistencAfterCommit) {
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

  public void checkTrackedViewsAfterCommit() {
    List<TV> values = store.getAllReadOnly(null);
    for (TrackedView<TV> view : viewMap.values()) {
      view.checkView(values);
    }
  }

  public void clearViews() {
    for (TrackedView<TV> view : viewMap.values()) {
      view.clear();
    }
  }

  public <VT extends TrackedView<TV>> VT getView(Class<VT> viewType) {
    VT view = store.computeAtomic(() -> getAndCloneView(viewType));
    JacisStoreTxView<K, TV, CV> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
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
      TrackedView<TV> subView = clusteredView.getSubView(subviewKey);
      return (VT) subView.clone();
    });
    JacisStoreTxView<K, TV, CV> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
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
    VT viewClone = (VT) view.clone();
    return viewClone;
  }

  private void initTrackedView(TrackedView<TV> view) {
    for (TV val : store.getAllReadOnly(null)) {
      view.trackModification(null, val);
    }
  }

}
