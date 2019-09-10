package store.jesframework.readmodel.repository;

import java.util.UUID;

import javax.annotation.Nonnull;

import org.springframework.data.jpa.repository.JpaRepository;

import store.jesframework.readmodel.model.Item;

public interface ItemRepository extends JpaRepository<Item, Long> {

    Item findByUuid(@Nonnull UUID uuid);

}
