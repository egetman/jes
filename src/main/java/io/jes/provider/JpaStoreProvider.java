package io.jes.provider;

import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;
import io.jes.provider.jpa.StoreEntry;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.SerializerFactory;

@SuppressWarnings("JpaQlInspection")
public class JpaStoreProvider implements StoreProvider {

    private final EntityManager entityManager;
    private final EventSerializer<byte[]> serializer = SerializerFactory.newBinarySerializer();

    public JpaStoreProvider(@Nonnull EntityManager entityManager) {
        this.entityManager = Objects.requireNonNull(entityManager, "EntityManager must not be null");

    }

    @Override
    public Stream<Event> readFrom(long offset) {
        final TypedQuery<StoreEntry> query = entityManager.createQuery(
                "SELECT entity FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.id > :id ORDER BY entity.id",
                StoreEntry.class
        );

        query.setParameter("id", offset);
        return query.getResultStream().map(storeEntry -> serializer.deserialize(storeEntry.getData()));
    }

    @Override
    public Stream<Event> readBy(@Nonnull String stream) {
        final TypedQuery<StoreEntry> query = entityManager.createQuery(
                "SELECT entity FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.stream = :stream ORDER BY id",
                StoreEntry.class
        );

        query.setParameter("stream", stream);
        return query.getResultStream().map(storeEntry -> serializer.deserialize(storeEntry.getData()));
    }

    @Override
    public void write(@Nonnull Event event) {
        final String stream = event.stream();
        final long expectedVersion = event.expectedStreamVersion();
        if (stream != null && expectedVersion != -1) {
            TypedQuery<Long> versionQuery = entityManager.createQuery(
                    "SELECT COUNT(entity) FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.stream = :stream",
                    Long.class
            );

            final Long actualVersion = versionQuery.getSingleResult();
            if (expectedVersion != actualVersion) {
                throw new VersionMismatchException(expectedVersion, actualVersion);
            }
        }
        final byte[] data = serializer.serialize(event);
        final StoreEntry storeEntry = new StoreEntry(stream, data);

        final EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();
            entityManager.persist(storeEntry);
        } catch (Exception e) {
            transaction.setRollbackOnly();
            throw e;
        } finally {
            transaction.commit();
        }
    }

}
