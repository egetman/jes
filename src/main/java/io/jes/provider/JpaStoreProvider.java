package io.jes.provider;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;

import io.jes.Event;
import io.jes.ex.BrokenStoreException;
import io.jes.ex.VersionMismatchException;
import io.jes.provider.jpa.StoreEntry;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.SerializerFactory;

@SuppressWarnings("JpaQlInspection")
public class JpaStoreProvider<T> implements StoreProvider {

    private final EntityManager entityManager;
    private final EventSerializer<T> serializer;

    public JpaStoreProvider(@Nonnull EntityManager entityManager,  @Nonnull Class<T> serializationType) {
        this.entityManager = Objects.requireNonNull(entityManager, "EntityManager must not be null");
        if (serializationType != byte[].class) {
            throw new IllegalArgumentException(serializationType + " serialization don't supported for " + getClass());
        }
        this.serializer = SerializerFactory.newEventSerializer(Objects.requireNonNull(serializationType));
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        final TypedQuery<StoreEntry> query = entityManager.createQuery(
                "SELECT entity FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.id > :id ORDER BY id",
                StoreEntry.class
        );

        query.setParameter("id", offset);
        //noinspection unchecked
        return query.getResultStream().map(storeEntry -> serializer.deserialize((T) storeEntry.getData()));
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        final TypedQuery<StoreEntry> query = entityManager.createQuery(
                "SELECT entity FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.uuid = :uuid ORDER BY id",
                StoreEntry.class
        );

        query.setParameter("uuid", uuid);
        //noinspection unchecked
        return query.getResultStream()
                .map(storeEntry -> serializer.deserialize((T) storeEntry.getData()))
                .collect(Collectors.toList());
    }

    @Override
    public void write(@Nonnull Event event) {
        final UUID uuid = event.uuid();
        final long expectedVersion = event.expectedStreamVersion();
        if (uuid != null && expectedVersion != -1) {
            TypedQuery<Long> versionQuery = entityManager.createQuery(
                    "SELECT COUNT(entity) FROM io.jes.provider.jpa.StoreEntry entity WHERE entity.uuid = :uuid",
                    Long.class
            );
            versionQuery.setParameter("uuid", uuid);
            final Long actualVersion = versionQuery.getSingleResult();
            if (expectedVersion != actualVersion) {
                throw new VersionMismatchException(expectedVersion, actualVersion);
            }
        }
        final byte[] data = (byte[]) serializer.serialize(event);
        final StoreEntry storeEntry = new StoreEntry(uuid, data);

        final EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();
            entityManager.persist(storeEntry);
        } catch (Exception e) {
            transaction.setRollbackOnly();
            throw new BrokenStoreException(e);
        } finally {
            transaction.commit();
        }
    }

}
