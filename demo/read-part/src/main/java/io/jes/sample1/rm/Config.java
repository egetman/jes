package io.jes.sample1.rm;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import io.jes.JEventStore;
import io.jes.lock.InMemoryReentrantLock;
import io.jes.lock.Lock;
import io.jes.offset.InMemoryOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.StoreProvider;
import io.jes.sample1.rm.event.ItemCreated;
import io.jes.sample1.rm.event.ItemRemoved;
import io.jes.sample1.rm.event.OrderPlaced;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.SerializationOptions;
import io.jes.serializer.TypeRegistry;

@Configuration
@EnableJpaRepositories(basePackages = "io.jes.sample1.rm")
@EnableAutoConfiguration
public class Config {

    @Bean
    public StoreProvider jdbcStoreProvider(DataSource dataSource, SerializationOption[] options) {
        return new JdbcStoreProvider<>(dataSource, String.class, options);
    }

    @Bean
    public JEventStore eventStore(StoreProvider storeProvider) {
        return new JEventStore(storeProvider);
    }

    @Bean
    public Offset offset() {
        return new InMemoryOffset();
    }

    @Bean
    public Lock lockManager() {
        return new InMemoryReentrantLock();
    }

    // U can use any aliases for events. So u don't need to hardcode serialized class name of event.
    // Every client can create it's own model to deserialize events from event store just using aliasing.
    @Bean
    public SerializationOption[] serializationOptions() {
        final TypeRegistry registry = new TypeRegistry();
        registry.addAlias(ItemCreated.class, ItemCreated.class.getSimpleName());
        registry.addAlias(ItemRemoved.class, "ItemRemoved");
        registry.addAlias(OrderPlaced.class, OrderPlaced.class.getSimpleName());
        return new SerializationOption[]{SerializationOptions.USE_TYPE_ALIASES, registry};
    }

}
