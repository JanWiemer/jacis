package org.jacis.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.JacisTransactionListener;
import org.jacis.plugin.JacisTransactionListenerAdapter;
import org.jacis.trackedviews.TrackedView;
import org.jacis.trackedviews.TrackedViewClustered;

/**
 * @author Jan Wiemer
 * 
 * Registry where tracked views can be registered for an object store.
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public class TrackedViewRegistry<K, V> implements JacisModificationListener<K, V> {

  private final JacisStore<K, V> store;
  private final Map<Class<? extends TrackedView<V>>, TrackedView<V>> viewMap = new HashMap<>();
  private final JacisTransactionListener txListener = new JacisTransactionListenerAdapter() {

    @Override
    public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
      checkTrackedViewsAfterCommit();
    }

  };

  public TrackedViewRegistry(JacisStore<K, V> store, boolean checkConsistencAfterCommit) {
    this.store = store;
    if (checkConsistencAfterCommit) {
      store.getContainer().registerTransactionListener(txListener);
    }

  }

  @SuppressWarnings("unchecked")
  public void registerTrackedView(TrackedView<V> view) {
    if (!store.getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering tracked views is only possible if the store is keeping track of the original values of a transactional view.");
    }
    store.executeAtomic(() -> initTrackedView(view));
    viewMap.put((Class<? extends TrackedView<V>>) view.getClass(), view);
  }

  public Collection<TrackedView<V>> getAllViews() {
    return viewMap.values();
  }

  public Set<Class<? extends TrackedView<V>>> getAllViewClasses() {
    return viewMap.keySet();
  }

  public boolean containsView(Class<? extends TrackedView<V>> viewType) {
    return viewMap.containsKey(viewType);
  }

  @Override
  public void onModification(K key, V oldValue, V newValue, JacisTransactionHandle tx) {
    for (TrackedView<V> view : viewMap.values()) {
      view.trackModification(oldValue, newValue);
    }
  }

  public void checkTrackedViewsAfterCommit() {
    List<V> values = store.getAllReadOnly(null);
    for (TrackedView<V> view : viewMap.values()) {
      view.checkView(values);
    }
  }

  public void clearViews() {
    for (TrackedView<V> view : viewMap.values()) {
      view.clear();
    }
  }

  public <VT extends TrackedView<V>> VT getView(Class<VT> viewType) {
    VT view = store.computeAtomic(() -> getAndCloneView(viewType));
    JacisStoreTxView<K, V> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, V> entryTxView : txView.getAllEntryTxViews()) {
        view.trackModification(entryTxView.getOrigValue(), entryTxView.getValue());
      }
    }
    return view;
  }

  @SuppressWarnings("unchecked")
  public <SVK> Collection<SVK> getSubViewKeys(Class<? extends TrackedViewClustered<V, SVK, ? extends TrackedView<V>>> viewType) {
    return store.computeAtomic(() -> {
      TrackedView<V> view = viewMap.get(viewType);
      if (view == null) {
        throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
      } else if (!viewType.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type! view: " + view);
      } else if (!TrackedViewClustered.class.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of " + TrackedViewClustered.class + "! view: " + view);
      }
      TrackedViewClustered<V, SVK, ? extends TrackedView<V>> clusteredView = (TrackedViewClustered<V, SVK, ? extends TrackedView<V>>) view;
      return clusteredView.getSubViewKeys();
    });
  }

  @SuppressWarnings("unchecked")
  public <VT extends TrackedView<V>, VK> VT getSubView(Class<? extends TrackedViewClustered<V, VK, VT>> viewType, VK subviewKey) {
    VT subViewClone = store.computeAtomic(() -> {
      TrackedView<V> view = viewMap.get(viewType);
      if (view == null) {
        throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
      } else if (!viewType.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type! view: " + view);
      } else if (!TrackedViewClustered.class.isInstance(view)) {
        throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of " + TrackedViewClustered.class + "! view: " + view);
      }
      TrackedViewClustered<V, VK, TrackedView<V>> clusteredView = (TrackedViewClustered<V, VK, TrackedView<V>>) view;
      TrackedView<V> subView = clusteredView.getSubView(subviewKey);
      return (VT) subView.clone();
    });
    JacisStoreTxView<K, V> txView = store.getTxView();
    if (txView != null) {
      for (StoreEntryTxView<K, V> entryTxView : txView.getAllEntryTxViews()) {
        subViewClone.trackModification(entryTxView.getOrigValue(), entryTxView.getValue());
      }
    }
    return subViewClone;
  }

  @SuppressWarnings("unchecked")
  private <VT extends TrackedView<V>> VT getAndCloneView(Class<? extends TrackedView<V>> viewType) {
    VT view = (VT) viewMap.get(viewType);
    if (view == null) {
      throw new IllegalArgumentException("No tracked view of type " + viewType + " registered! All registered views: " + viewMap.keySet());
    } else if (!viewType.isInstance(view)) {
      throw new IllegalArgumentException("The view registered for the type " + viewType + " is no instance of this type: " + view);
    }
    VT viewClone = (VT) view.clone();
    return viewClone;
  }

  private void initTrackedView(TrackedView<V> view) {
    for (V val : store.getAllReadOnly(null)) {
      view.trackModification(null, val);
    }
  }

}
