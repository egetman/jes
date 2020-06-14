package store.jesframework.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import store.jesframework.Event;
import store.jesframework.ex.BrokenStoreException;
import store.jesframework.ex.VersionMismatchException;
import store.jesframework.provider.jpa.StoreEntry;
import store.jesframework.provider.jpa.StoreEntryFactory;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.serializer.api.Serializer;
import store.jesframework.serializer.impl.SerializerFactory;
import store.jesframework.snapshot.SnapshotReader;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * JPA {@link StoreProvider} implementation.
 * {@implNote this implementation works correctly with application-managed entity manager factory && disabled
 * autocommit feature — JpaStoreProvider manage transactions manually}.
 *
 * @param <T> type of event serialization.
 */
@Slf4j
public class JpaStoreProvider<T> implements StoreProvider, SnapshotReader, AutoCloseable {

    private static final int FETCH_SIZE = 1000;
    private static final String READ_ONLY_HINT = "org.hibernate.readOnly";
    private static final String FETCH_SIZE_HINT = "org.hibernate.fetchSize";

    private final Serializer<Event, T> serializer;
    private final EntityManagerFactory entityManagerFactory;

    private final Class<? extends StoreEntry> entryType;

    private static final String QUERY_BY_UUID = "SELECT e FROM %s e WHERE e.uuid = :uuid ORDER BY id";
    private static final String DELETE_BY_UUID = "DELETE FROM %s e WHERE e.uuid = :uuid";
    private static final String QUERY_COUNT_BY_UUID = "SELECT COUNT(e) FROM %s e WHERE e.uuid = :uuid";
    private static final String QUERY_BY_OFFSET = "SELECT e FROM %s e WHERE e.id > :id ORDER BY id";

    public JpaStoreProvider(@Nonnull EntityManagerFactory entityManagerFactory,
                            @Nullable SerializationOption... options) {
        try {
            this.entityManagerFactory = requireNonNull(entityManagerFactory, "EntityManagerFactory must not be null");
            this.serializer = SerializerFactory.newEventSerializer(options);

            final Format format = serializer.format();
            this.entryType = StoreEntryFactory.entryTypeOf(format.getJavaType());
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return doInTransactionAndKeepAlive((entityManager, transaction) -> {
            final TypedQuery<? extends StoreEntry> query = entityManager.createQuery(
                    format(QUERY_BY_OFFSET, entryType.getName()), entryType
            );

            query.setParameter("id", offset);
            query.setHint(READ_ONLY_HINT, true);
            query.setHint(FETCH_SIZE_HINT, FETCH_SIZE);

            return query.getResultStream()
                    .map(storeEntry -> serializer.deserialize(storeEntry.getData()))
                    .onClose(() -> {
                        try {
                            transaction.commit();
                            entityManager.close();
                        } catch (Exception e) {
                            if (transaction.isActive()) {
                                transaction.rollback();
                            }
                            throw new BrokenStoreException(e);
                        }
                    });
        });
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return readBy(uuid, 0);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        return doInTransaction(entityManager -> {
            final TypedQuery<? extends StoreEntry> query = entityManager.createQuery(
                    format(QUERY_BY_UUID, entryType.getName()), entryType
            );

            query.setParameter("uuid", uuid);
            query.setMaxResults(MAX_VALUE);
            query.setFirstResult((int) skip);
            query.setHint(READ_ONLY_HINT, true);
            query.setHint(FETCH_SIZE_HINT, FETCH_SIZE);
            return query.getResultStream()
                    .map(storeEntry -> serializer.deserialize(storeEntry.getData()))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public void write(@Nonnull Event event) {
        final UUID uuid = event.uuid();
        final long expectedVersion = event.expectedStreamVersion();
        if (uuid != null && expectedVersion != -1) {

            final long actualVersion = doInTransaction(entityManager -> {
                final TypedQuery<Long> versionQuery = entityManager.createQuery(
                        format(QUERY_COUNT_BY_UUID, entryType.getName()), Long.class
                );
                versionQuery.setParameter("uuid", uuid);
                versionQuery.setHint(READ_ONLY_HINT, true);

                return versionQuery.getSingleResult();
            });

            if (expectedVersion != actualVersion) {
                throw new VersionMismatchException(uuid, expectedVersion, actualVersion);
            }
        }

        final T data = serializer.serialize(event);
        final StoreEntry entry = StoreEntryFactory.newEntry(uuid, data);
        final Consumer<EntityManager> consumer = entityManager -> entityManager.persist(entry);
        doInTransaction(consumer);
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        log.trace("Prepare to remove {} event stream", uuid);

        final int affectedEvents = doInTransaction(entityManager -> {
            final Query query = entityManager.createQuery(format(DELETE_BY_UUID, entryType.getName()));
            query.setParameter("uuid", uuid);

            return query.executeUpdate();
        });
        log.trace("{} events successfully removed", affectedEvents);
    }

    private <R> R doInTransaction(@Nonnull Function<EntityManager, R> action) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        final EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            final R result = action.apply(entityManager);
            transaction.commit();
            return result;
        } catch (Exception e) {
            transaction.rollback();
            throw new BrokenStoreException(e);
        } finally {
            entityManager.close();
        }
    }

    private void doInTransaction(@Nonnull Consumer<EntityManager> action) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        final EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            action.accept(entityManager);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            throw new BrokenStoreException(e);
        } finally {
            entityManager.close();
        }
    }

    private <R> R doInTransactionAndKeepAlive(@Nonnull BiFunction<EntityManager, EntityTransaction, R> action) {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        final EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            return action.apply(entityManager, transaction);
        } catch (Exception e) {
            transaction.rollback();
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public void close() {
        try {
            entityManagerFactory.close();
        } catch (Exception e) {
            log.error("Failed to close resource:", e);
        }
    }

}
