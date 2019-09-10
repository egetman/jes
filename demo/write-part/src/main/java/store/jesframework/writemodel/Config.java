package store.jesframework.writemodel;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import store.jesframework.AggregateStore;
import store.jesframework.JEventStore;
import store.jesframework.bus.CommandBus;
import store.jesframework.bus.SyncCommandBus;
import store.jesframework.lock.JdbcLock;
import store.jesframework.lock.Lock;
import store.jesframework.offset.JdbcOffset;
import store.jesframework.offset.Offset;
import store.jesframework.provider.JdbcStoreProvider;
import store.jesframework.provider.StoreProvider;
import store.jesframework.writemodel.event.ItemCreated;
import store.jesframework.writemodel.event.ItemRemoved;
import store.jesframework.writemodel.event.OrderPlaced;
import store.jesframework.serializer.SerializationOption;
import store.jesframework.serializer.SerializationOptions;
import store.jesframework.serializer.TypeRegistry;

@Configuration
@EnableAutoConfiguration
public class Config {

    @Bean(destroyMethod = "close")
    @SuppressWarnings("ContextJavaBeanUnresolvedMethodsInspection")
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
