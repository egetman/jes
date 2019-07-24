package io.jes.sample1.rm.model;

import java.time.LocalDateTime;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "item", schema = "stock")
@EqualsAndHashCode(of = "uuid")
public class Item {

    @Id
    @SequenceGenerator(name = "item_generator", sequenceName = "stock.item_sequence")
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "item_generator")
    @Column(name = "ID", unique = true, nullable = false, updatable = false)
    private Long id;

    @Column(name = "uuid", unique = true, updatable = false)
    private UUID uuid;

    @Column(name = "name")
    private String name = "";

    @Column(name = "quantity")
    private long quantity;

    @Column(name = "last_ordered")
    private LocalDateTime lastOrdered;
}
