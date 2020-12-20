/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.example;

import java.util.List;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.example.JacisExample1.Account;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.store.JacisStore;
import org.jacis.trackedviews.TrackedView;

/**
 * Example 3: using a tracked view.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({ "WeakerAccess", "CodeBlock2Expr" })
public class JacisExample3 {

  // Note that we use the same account object introduced for the first example

  public static void main(String[] args) {
    JacisContainer container = new JacisContainer();
    JacisObjectTypeSpec<String, Account, Account> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, Account.class, new JacisCloningObjectAdapter<>());
    JacisStore<String, Account> store = container.createStore(objectTypeSpec).getStore();

    // First register the tracked view

    store.getTrackedViewRegistry().registerTrackedView(new TotalBalanceView());

    // First we create some accounts to have some test data...

    container.withLocalTx(() -> {
      store.update("account1", new Account("account1").deposit(-100));
      store.update("account2", new Account("account2").deposit(10));
      store.update("account3", new Account("account3").deposit(100));
    });

    // on internalCommit the tracked view is updated automatically
    TotalBalanceView view0 = store.getTrackedViewRegistry().getView(TotalBalanceView.class);
    System.out.println("tracked balance=" + view0.getTotalBalance());

    // inside a transaction the transaction local view of the values is respected
    container.withLocalTx(() -> {
      store.update("account1", store.get("account1").deposit(1000));
      store.update("account4", new Account("account4").deposit(101));
      // note that the getView method takes a snapshot at the time it is called...
      TotalBalanceView view1 = store.getTrackedViewRegistry().getView(TotalBalanceView.class);
      System.out.println("tracked balance=" + view1.getTotalBalance());
      // later updates are not tracked by this snapshot
      store.update("account1", store.get("account1").deposit(1000));
      System.out.println("tracked balance old snapshot=" + view1.getTotalBalance());
      TotalBalanceView view2 = store.getTrackedViewRegistry().getView(TotalBalanceView.class);
      System.out.println("tracked balance new snapshot=" + view2.getTotalBalance());
    });

  }

  public static class TotalBalanceView implements TrackedView<Account> {

    private long totalBalance = 0;

    @SuppressWarnings("unchecked")
    @Override
    public TrackedView<Account> clone() {
      try {
        return (TrackedView<Account>) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException("clone failed");
      }
    }

    @Override
    public void trackModification(Account oldValue, Account newValue) {
      totalBalance += newValue == null ? 0 : newValue.getBalance();
      totalBalance -= oldValue == null ? 0 : oldValue.getBalance();
    }

    @Override
    public void checkView(List<Account> values) {
      long checkValue = values.stream().mapToLong(Account::getBalance).sum();
      if (totalBalance != checkValue) {
        throw new IllegalStateException("Corrupt view! Tracked value=" + totalBalance + " computed value=" + checkValue);
      }
    }

    @Override
    public void clear() {
      totalBalance = 0;
    }

    public long getTotalBalance() {
      return totalBalance;
    }

  }

}
