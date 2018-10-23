package io.jes.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import io.jes.Event;
import io.jes.ex.BrokenStoreException;
import io.jes.ex.VersionMismatchException;
import io.jes.provider.jpa.StoreEntry;
import io.jes.provider.jpa.StoreEntryFactory;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.SerializerFactory;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * JPA {@link StoreProvider} implementation.
 *
 * @param <T> type of event serialization.
 */
@Slf4j
public class JpaStoreProvider<T> implements StoreProvider {

    private final EntityManager entityManager;
    private final EventSerializer<T> serializer;

    private final Class<? extends StoreEntry> entryType;

    private static final String QUERY_BY_UUID = "SELECT e FROM %s e WHERE e.uuid = :uuid ORDER BY id";
    private static final String DELETE_BY_UUID = "DELETE FROM %s e WHERE e.uuid = :uuid";
    private static final String QUERY_COUNT_BY_UUID = "SELECT COUNT(e) FROM %s e WHERE e.uuid = :uuid";
    private static final String QUERY_BY_OFFSET = "SELECT e FROM %s e WHERE e.id > :id ORDER BY id";

    public JpaStoreProvider(@Nonnull EntityManager entityManager, @Nonnull Class<T> serializationType,
                            @Nonnull SerializationOption... options) {

        this.entityManager = requireNonNull(entityManager, "EntityManager must not be null");
        this.serializer = SerializerFactory.newEventSerializer(serializationType, options);
        this.entryType = StoreEntryFactory.entryTypeOf(serializationType);
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        final TypedQuery<? extends StoreEntry> query = entityManager.createQuery(
                format(QUERY_BY_OFFSET, entryType.getName()), entryType
        );

        query.setParameter("id", offset);
        return query.getResultStream().map(storeEntry -> serializer.deserialize(storeEntry.getData()));
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        final TypedQuery<? extends StoreEntry> query = entityManager.createQuery(
                format(QUERY_BY_UUID, entryType.getName()), entryType
        );

        query.setParameter("uuid", uuid);
        return query.getResultStream()
                .map(storeEntry -> serializer.deserialize(storeEntry.getData()))
                .collect(Collectors.toList());
    }

    @Override
    public void write(@Nonnull Event event) {
        final UUID uuid = event.uuid();
        final long expectedVersion = event.expectedStreamVersion();
        if (uuid != null && expectedVersion != -1) {
            final TypedQuery<Long> versionQuery = entityManager.createQuery(
                    format(QUERY_COUNT_BY_UUID, entryType.getName()), Long.class
            );
            versionQuery.setParameter("uuid", uuid);
            final Long actualVersion = versionQuery.getSingleResult();
            if (expectedVersion != actualVersion) {
                throw new VersionMismatchException(expectedVersion, actualVersion);
            }
        }

        final T data = serializer.serialize(event);
        final StoreEntry entry = StoreEntryFactory.newEntry(uuid, data);
        doInTransaction(() -> entityManager.persist(entry));
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        log.warn("Prepare to remove {} event stream", uuid);
        final Query query = entityManager.createQuery(format(DELETE_BY_UUID, entryType.getName()));
        query.setParameter("uuid", uuid);

        doInTransaction(() -> {
            final int affectedEvents = query.executeUpdate();
            log.warn("{} events successfully removed", affectedEvents);
        });
    }

    private void doInTransaction(@Nonnull Runnable action) {
        final EntityTransaction transaction = entityManager.getTransaction();
        try {
            transaction.begin();
            action.run();
        } catch (Exception e) {
            transaction.setRollbackOnly();
            throw new BrokenStoreException(e);
        } finally {
            transaction.commit();
        }
    }

}
