package io.jes.provider;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.Event;
import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.InfrastructureFactory.newEntityManager;
import static java.util.Arrays.asList;

@Slf4j
class JpaStoreProviderTest {

    @Test
    void shouldReadOwnWrites() {
        final StoreProvider provider = new JpaStoreProvider(newEntityManager());

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(Collectors.toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

}