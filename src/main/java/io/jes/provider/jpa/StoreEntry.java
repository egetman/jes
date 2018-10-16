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
import javax.persistence.Lob;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import static lombok.AccessLevel.PROTECTED;

/**
 * Simple JPA entity for storing event data.
 */
@Getter
@Setter
@Entity
@Table(name = "event_store")
@SuppressWarnings({"JpaDataSourceORMInspection"})
@NoArgsConstructor(access = PROTECTED, force = true)
public class StoreEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", unique = true, updatable = false)
    private final Long id;

    @Column(name = "uuid", updatable = false)
    private final UUID uuid;

    @Lob
    @Column(name = "data", nullable = false, updatable = false)
    private final byte[] data;

    public StoreEntry(@Nullable UUID uuid, @Nonnull byte[] data) {
        this.id = null;
        this.uuid = uuid;
        this.data = Objects.requireNonNull(data, "Event data can't be null");
    }
}
