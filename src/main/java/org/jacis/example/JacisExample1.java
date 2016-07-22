package org.jacis.example;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.plugin.objectadapter.cloning.JacisCloneable;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.plugin.objectadapter.readonly.AbstractReadOnlyModeSupportingObject;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;

/**
 * @author Jan Wiemer
 * 
 * Example 1: showing basic usage of a JACIS store.
 *
 */
public class JacisExample1 {

  // First we create a simple example class of objects that shall be stored in a transactional store.
  // The class implements the JacisCloneable interface to enable the store to clone the object without using reflection.
  // Furthermore the class extends the AbstractReadOnlyModeSupportingObject (implementing the JacisReadonlyModeSupport interface). 
  // This means the object provides a secure read only mode. 
  // It is the responsibility of an object implementing the JacisReadonlyModeSupport interface
  // to prevent any modification after the method switchToReadOnlyMode is called.
  // The AbstractReadOnlyModeSupportingObject provides a checkWritable method throwing an exception
  // if the object is in read only mode.

  static class Account extends AbstractReadOnlyModeSupportingObject implements JacisCloneable<Account> {

    private final String name;
    private long balance;

    public Account(String name) {
      this.name = name;
    }

    @Override
    public Account clone() {
      return (Account) super.clone();
    }

    Account deposit(long amount) {
      checkWritable();
      balance += amount;
      return this;
    }

    public Account withdraw(long amount) {
      checkWritable();
      balance -= amount;
      return this;
    }

    public String getName() {
      return name;
    }

    public long getBalance() {
      return balance;
    }

  }

  public static void main(String[] args) {
    // first we initialize a JACIS container:
    JacisContainer container = new JacisContainer();

    // now we create a store for our example object:
    JacisObjectTypeSpec<String, Account, Account> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, Account.class, new JacisCloningObjectAdapter<Account>());
    JacisStore<String, Account, Account> store = container.createStore(objectTypeSpec);

    // now we start a transaction
    JacisLocalTransaction tx = container.beginLocalTransaction();
    // and create a new Account
    Account account1 = new Account("account1");
    // modifications (and creation of new objects) have to be notified to the store by calling:
    store.update(account1.getName(), account1);
    // now we commit the transaction. Afterwards all other transactions can see our new Account
    tx.commit();

    // we use a helper method executing some code inside a transaction to deposit some money on the account.
    // Note that the update is necessary, otherwise the change will be lost after commit (try it)
    container.withLocalTx(() -> {
      Account acc = store.get("account1");
      acc.deposit(100);
      store.update("account1", acc);
    });

    // now we use another transaction to check the balance of the Account
    container.withLocalTx(() -> {
      Account acc = store.get("account1");
      System.out.println("balance of " + acc.getName() + ": " + acc.getBalance());
    });

    // now we withdraw some money and simulate an exception causing the transaction to be rolled back
    try {
      container.withLocalTx(() -> {
        Account acc = store.get("account1");
        acc.withdraw(10);
        store.update("account1", acc);
        throw new RuntimeException("Error in transaction!");
      });
    } catch (RuntimeException e) {
      System.out.println("Expected exception " + e);
      // expected
    }

    // now we again check the balance of the Account to see nothing is withdrawn
    container.withLocalTx(() -> {
      Account acc = store.get("account1");
      System.out.println("balance of " + acc.getName() + ": " + acc.getBalance());
    });

  }
}
