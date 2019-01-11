package io.jes.provider.jpa;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.MappedSuperclass;
import javax.persistence.Table;
import javax.persistence.Transient;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import static lombok.AccessLevel.PROTECTED;

/**
 * Simple JPA entity for storing event data.
 */
@Getter
@Setter
@MappedSuperclass
@EqualsAndHashCode(of = "uuid")
@NoArgsConstructor(access = PROTECTED, force = true)
public abstract class StoreEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", unique = true, updatable = false)
    private final Long id;

    @Column(name = "uuid", updatable = false)
    private final UUID uuid;

    StoreEntry(@Nullable UUID uuid) {
        this.id = null;
        this.uuid = uuid;
    }

    @Nonnull
    @Transient
    public abstract <T> T getData();

    @Getter
    @Setter
    @Entity
    @NoArgsConstructor(access = PROTECTED, force = true)
    @EqualsAndHashCode(exclude = {"data"}, callSuper = true)
    @Table(name = "event_store", indexes = {@Index(name = "uuid_idx", columnList = "uuid")})
    static class StoreBinaryEntry extends StoreEntry {

        @Column(name = "data", nullable = false, updatable = false, columnDefinition = "BYTEA")
        private final byte[] data;

        StoreBinaryEntry(@Nullable UUID uuid, @Nonnull byte[] data) {
            super(uuid);
            this.data = Objects.requireNonNull(data, "Event data can't be null");
        }
    }

    @Getter
    @Setter
    @Entity
    @NoArgsConstructor(access = PROTECTED, force = true)
    @EqualsAndHashCode(exclude = {"data"}, callSuper = true)
    @Table(name = "event_store", indexes = {@Index(name = "uuid_idx", columnList = "uuid")})
    static class StoreStringEntry extends StoreEntry {

        @Column(name = "data", nullable = false, updatable = false, columnDefinition = "TEXT")
        private final String data;

        StoreStringEntry(@Nullable UUID uuid, @Nonnull String data) {
            super(uuid);
            this.data = Objects.requireNonNull(data, "Event data can't be null");
        }

    }

}
