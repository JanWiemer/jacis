[![Build and test](https://github.com/JanWiemer/jacis/actions/workflows/buildDeploy.yml/badge.svg)](https://github.com/JanWiemer/jacis/actions/workflows/buildDeploy.yml)

# JACIS

Java ACI Store - a transient, in-memory store for Java objects with transactional behavior.

The name is derived from ACID (Atomicity, Consistency, Isolation, Durability). JACIS provides the first three properties; it does not provide durability.

## Requirements

JACIS targets Java 11 or newer. The project build requires JDK 17 or newer; CI builds and tests with JDK 17, 21, and 25.

## Install

JACIS is published to Maven Central. Use version `2.1.22` or a newer release.

**Gradle (Groovy DSL):**

```groovy
dependencies {
    implementation 'io.github.janwiemer:jacis:2.1.22'
}
```

**Maven:**

```xml
<dependency>
    <groupId>io.github.janwiemer</groupId>
    <artifactId>jacis</artifactId>
    <version>2.1.22</version>
</dependency>
```

## Quick start

Create a container and register a store for your value type. When using the cloning adapter, the value type must implement `JacisCloneable`.

```java
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.plugin.objectadapter.cloning.JacisCloneable;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;

class QuickStart {
    public static void main(String[] args) {
        JacisContainer container = new JacisContainer();
        JacisObjectTypeSpec<String, Account, Account> spec =
            new JacisObjectTypeSpec<>(String.class, Account.class);
        JacisStore<String, Account> store = container.createStore(spec).getStore();

        JacisLocalTransaction tx = container.beginLocalTransaction();
        store.update("account-1", new Account("account-1"));
        tx.commit();
    }

    static class Account implements JacisCloneable<Account> {
        private final String id;

        Account(String id) { this.id = id; }

        @Override
        public Account clone() { return new Account(id); }
    }
}
```

See the [installation guide](https://github.com/JanWiemer/jacis/wiki/Installation) and [JACIS wiki](https://github.com/JanWiemer/jacis/wiki) for complete examples, transaction details, and extensions.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions and the contribution process.

## Project policies

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security policy](SECURITY.md)
- [License](LICENSE)

JACIS follows [Semantic Versioning](https://semver.org/); public API is indicated by the `JacisApi` annotation.
