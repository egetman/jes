package io.jes.sample1.rm.repository;

import java.util.UUID;

import javax.annotation.Nonnull;

import org.springframework.data.jpa.repository.JpaRepository;

import io.jes.sample1.rm.model.Item;

public interface ItemRepository extends JpaRepository<Item, Long> {

    Item findByUuid(@Nonnull UUID uuid);

}
