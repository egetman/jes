package store.jesframework.readmodel;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import store.jesframework.JEventStore;
import store.jesframework.lock.JdbcLock;
import store.jesframework.lock.Lock;
import store.jesframework.offset.JdbcOffset;
import store.jesframework.offset.Offset;
import store.jesframework.provider.JdbcStoreProvider;
import store.jesframework.provider.StoreProvider;
import store.jesframework.readmodel.event.ItemCreated;
import store.jesframework.readmodel.event.ItemRemoved;
import store.jesframework.readmodel.event.OrderPlaced;
import store.jesframework.serializer.TypeRegistry;
import store.jesframework.serializer.api.SerializationOption;

@Configuration
@EnableJpaRepositories(basePackages = "store.jesframework.readmodel.*")
@EnableAutoConfiguration
public class Config {

    @Bean(destroyMethod = "close")
    @SuppressWarnings("ContextJavaBeanUnresolvedMethodsInspection")
    public StoreProvider jdbcStoreProvider(DataSource dataSource, SerializationOption[] options) {
        return new JdbcStoreProvider<>(dataSource, options);
    }

    @Bean
    public JEventStore eventStore(StoreProvider storeProvider) {
        return new JEventStore(storeProvider);
    }

    @Bean
    public Offset offset(DataSource dataSource) {
        return new JdbcOffset(dataSource);
    }

    @Bean
    public Lock lock(DataSource dataSource) {
        return new JdbcLock(dataSource);
    }

    // U can use any aliases for events. So u don't need to hardcode serialized class name of event.
    // Every client can create its own model to deserialize events from event store just using aliasing.
    @Bean
    public SerializationOption[] serializationOptions() {
        final TypeRegistry registry = new TypeRegistry();
        registry.addAlias(ItemCreated.class, ItemCreated.class.getSimpleName());
        registry.addAlias(ItemRemoved.class, "ItemRemoved");
        registry.addAlias(OrderPlaced.class, OrderPlaced.class.getSimpleName());
        return new SerializationOption[]{registry};
    }

}
