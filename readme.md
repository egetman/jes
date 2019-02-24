# Jes 
###### Strongly inspired by:
* [Versioning in an Event Sourced System](https://leanpub.com/esversioning) by Greg Young
* [The dark side of event sourcing: Managing data conversion](https://ieeexplore.ieee.org/document/7884621) by Michiel Overeem, Marten Spoor & Slinger Jansen 

---
Jes is a library for those who wanted to try Event Sourcing but did not know how to approach this. 
It demonstrates well the basic principles inherent in approach and does not limit the methods of use.
How to use the library - everyone decides for himself, in accordance with his understanding of the concept of Event 
Sourcing.

Jes provides several abstractions, that helps organize workflow of your application.
 
---
## Getting started
Get the code by cloning the Git repository:
```sh
$ git clone https://github.com/egetman/jes.git
$ cd jes
```
Then build the code using Maven:
```
$ mvn clean install
```

Then you can use library as:
```xml
<dependency>
    <groupId>io.jes</groupId>
    <artifactId>jes-core</artifactId>
    <version>${jes.version}</version>
</dependency>
```

## Jes basics
Central library element is `JEventStore`. It provides basic functionality for managing events in your system:

```java
public class JEventStore {
    public JEventStore(@Nonnull StoreProvider provider) {
    	...
    }
    
    public Stream<Event> readFrom(long offset) {...}
    public Collection<Event> readBy(@Nonnull UUID uuid) {...}
    // etc...
}
```
`StoreProvider` is actual component that incapsulates all interaction with concrete store.  
Currently `StoreProvider` can be built on top of the database.  
There are 2 implementations of `StoreProvider` to use: `JdbcStoreProvider` & `JpaStoreProvider`:

```java
public class JdbcStoreProvider<T>... {
    public JdbcStoreProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType,
                             @Nonnull SerializationOption... options) {
}

public class JpaStoreProvider<T>... {
    public JpaStoreProvider(@Nonnull EntityManager entityManager, @Nonnull Class<T> serializationType, 
                            @Nonnull SerializationOption... options) {
}
```

`serializationType` can be `String.class` or `byte[].class`, which will create json / binary serializers implemented
 on top of Jackson / Kryo respectively.

To work with `Aggregates` you can use `AggregateStore`:

```java
public class AggregateStore { 
    
    public AggregateStore(@Nonnull JEventStore eventStore) {...}
    public AggregateStore(@Nonnull JEventStore eventStore, @Nonnull SnapshotProvider snapshotProvider) {...}

    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type) {...}
}
```

If `SnapshotProvider` specified, aggregate fetching is snapshotted.

`SnapshotProvider` can be any of `NoopSnapshotProvider`, `InMemorySnapshotProvider`, `JdbcSnapshotProvider`, 
`RedissonSnapshotProvider`.

There is also basic support for projectors via:
```java
public abstract class Projector extends Reactor { 
    public Projector(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull LockManager lockManager) {...}
}
```

To make it work - extend it and mark the needed methods as `Handler`:
```java
@Handler
private void handle(@Nonnull SmthHappend event) {
    ...
}
```

## Usage
If you are familiar with `Spring`, the typical configuration of `Jes` may look like:
```java
@Configuration
@EnableAutoConfiguration
public class JesConfig {

    @Bean
    public StoreProvider jdbcStoreProvider(DataSource dataSource) {
        return new JdbcStoreProvider<>(dataSource, String.class);
    }

    @Bean
    public JEventStore eventStore(StoreProvider storeProvider) {
        return new JEventStore(storeProvider);
    }

    @Bean
    public SnapshotProvider snapshotProvider() {
        return new InMemorySnapshotProvider();
    }

    @Bean
    public AggregateStore aggregateStore(JEventStore eventStore, SnapshotProvider snapshotProvider) {
        return new AggregateStore(eventStore, snapshotProvider);
    }

    @Bean
    public Offset offset() {
        // you can use RedissonOffset if you use Redisson
        return new InMemoryOffset();
    }

    @Bean
    public LockManager lockManager() {
        // you can use RedissonReentrantLockManager if you use Redisson
        return new InMemoryReentrantLockManager();
    }

    @Bean
    public Projector userProjector(JEventStore eventStore, Offset offset, LockManager lockManager) {
        return new UserProjector(eventStore, offset, lockManager);
    }
}

```

## Features
- [x] copy-and-replace event stream: event stream corrections
- [x] event stream split: event stream corrections
- [x] event stream merge: event stream corrections
- [x] event stream deletion: event stream corrections
- [ ] upcasting: versioning
- [x] copy-and-transform event store: versioning
- [x] strong/weak schema formats (partial): core
- [x] pull-based projectors: core
- [x] snapshotting: core

## Todo:
 - version caching? to avoid every-write check
 - snapshots invalidation: describe or reimplement
 - store structure validation on start
 - ~~event idempotency on read (clustered environment)~~ done by locking for projectors
 - string (xml) serializer (xstream?): use SerializationOption to specify type?
 - upcasting
 - ~~verify serialization/deserialization of abstract classes/interface references~~
 - ~~don't fail on unknown events~~ done via common event type for unregistered events
 
 ###### PRs are welcome