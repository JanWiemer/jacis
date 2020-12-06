
package org.jacis.container;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;

import org.jacis.store.JacisReadOnlyTransactionContext;
import org.jacis.store.JacisStore;

/**
 * Representing a read only version of all the contexts of the stores within a transaction belonging to the container.
 * 
 * @See JacisReadOnlyTransactionContext
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class JacisContainerReadOnlyTransactionContext {

  private final List<AbstractMap.SimpleImmutableEntry<JacisStore<?, ?>, JacisReadOnlyTransactionContext>> storeContextList = new ArrayList<>();

  void add(JacisStore<?, ?> store, JacisReadOnlyTransactionContext context) {
    storeContextList.add(new SimpleImmutableEntry<>(store, context));
  }

  void startReadOnlyTransactionWithContext() {
    for (SimpleImmutableEntry<JacisStore<?, ?>, JacisReadOnlyTransactionContext> entry : storeContextList) {
      entry.getKey().startReadOnlyTransactionWithContext(entry.getValue());
    }
  }
}
