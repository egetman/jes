package io.jes.provider.jpa;

import java.util.Objects;
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

    @Column(name = "stream", updatable = false)
    private final String stream;

    @Lob
    @Column(name = "data", nullable = false, updatable = false)
    private final byte[] data;

    public StoreEntry(@Nullable String stream, @Nonnull byte[] data) {
        this.id = null;
        this.stream = stream;
        this.data = Objects.requireNonNull(data, "Event data can't be null");
    }
}
