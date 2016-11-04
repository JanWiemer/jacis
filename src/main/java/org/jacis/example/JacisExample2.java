/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.example;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.example.JacisExample1.Account;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.store.JacisStore;

import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Example 2: showing stream API to access JACIS store.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({"WeakerAccess", "CodeBlock2Expr"})
public class JacisExample2 {

  // Note that we use the same account object introduced for the first example

  public static void main(String[] args) {
    JacisContainer container = new JacisContainer();
    JacisObjectTypeSpec<String, Account, Account> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, Account.class, new JacisCloningObjectAdapter<>());
    JacisStore<String, Account> store = container.createStore(objectTypeSpec).getStore();

    // First we create some accounts to have some test data...

    container.withLocalTx(() -> {
      store.update("account0", new Account("account0").withdraw(100));
      store.update("account1", new Account("account1").deposit(100));
      store.update("account2", new Account("account2").deposit(200));
      store.update("account3", new Account("account3").deposit(300));
      store.update("account4", new Account("account4").deposit(400));
      store.update("account5", new Account("account5").deposit(500));
      store.update("account6", new Account("account6").deposit(600));
      store.update("account7", new Account("account7").deposit(700));
      store.update("account8", new Account("account8").deposit(800));
      store.update("account9", new Account("account9").deposit(900));
    });

    // Now we show some examples how to use the stream API (note read only access is possible without a transaction)
    System.out.println("sum=" + store.streamReadOnly().mapToLong(Account::getBalance).sum());
    // starting with a filter
    System.out.println("#>500=" + store.streamReadOnly(acc -> acc.getBalance() > 500).count());
    // adding 10% interest
    container.withLocalTx(() -> {
      store.stream(acc -> acc.getBalance() > 0).forEach(acc -> {
        store.update(acc.getName(), acc.deposit(acc.getBalance() / 10));
      });
    });
    // output all accounts
    String str = store.streamReadOnly().//
        sorted(Comparator.comparing(Account::getName)). //
        map(acc -> acc.getName() + ":" + acc.getBalance()).//
        collect(Collectors.joining(", "));
    System.out.println("Accounts: " + str);
  }
}
