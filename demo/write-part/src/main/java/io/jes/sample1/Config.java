package io.jes.sample1;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.jes.AggregateStore;
import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.bus.SyncCommandBus;
import io.jes.lock.JdbcLock;
import io.jes.lock.Lock;
import io.jes.offset.JdbcOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.StoreProvider;
import io.jes.sample1.event.ItemCreated;
import io.jes.sample1.event.ItemRemoved;
import io.jes.sample1.event.OrderPlaced;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.SerializationOptions;
import io.jes.serializer.TypeRegistry;

@Configuration
@EnableAutoConfiguration
public class Config {

    @Bean
    public StoreProvider storeProvider(DataSource dataSource, SerializationOption[] options) {
        return new JdbcStoreProvider<>(dataSource, String.class, options);
    }

    @Bean
    public JEventStore eventStore(StoreProvider storeProvider) {
        return new JEventStore(storeProvider);
    }

    @Bean
    public CommandBus commandBus() {
        return new SyncCommandBus();
    }

    @Bean
    public Offset offset(DataSource dataSource) {
        return new JdbcOffset(dataSource);
    }

    @Bean
    public Lock lock(DataSource dataSource) {
        return new JdbcLock(dataSource);
    }

    @Bean
    public AggregateStore aggregateStore(JEventStore eventStore) {
        return new AggregateStore(eventStore);
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
