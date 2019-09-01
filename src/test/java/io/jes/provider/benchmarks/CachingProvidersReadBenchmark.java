package io.jes.provider.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.redisson.codec.JsonJacksonCodec;

import io.jes.Event;
import io.jes.provider.CachingStoreProvider;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.StoreProvider;
import io.jes.provider.cache.CaffeineCacheProvider;
import io.jes.provider.cache.InMemoryCacheProvider;
import io.jes.provider.cache.RedisCacheProvider;
import lombok.SneakyThrows;

import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.UUID.randomUUID;
import static java.util.stream.IntStream.range;

@Fork(1)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("DefaultAnnotationParam")
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class CachingProvidersReadBenchmark {

    @State(Scope.Benchmark)
    public static class Providers {

        @SuppressWarnings("FieldCanBeLocal")
        private int totalEventsToRead = 10000;

        private StoreProvider baseline;
        private StoreProvider inMemoryCacheProvider;
        private StoreProvider caffeineCacheProvider;
        private StoreProvider redisCacheProvider;

        @Setup(Level.Trial)
        public void setUp() {
            baseline = new JdbcStoreProvider<>(newPostgresDataSource(), String.class);
            inMemoryCacheProvider = new CachingStoreProvider(baseline, new InMemoryCacheProvider(totalEventsToRead));
            caffeineCacheProvider = new CachingStoreProvider(baseline, new CaffeineCacheProvider(totalEventsToRead));
            redisCacheProvider = new CachingStoreProvider(baseline, new RedisCacheProvider(newRedissonClient(),
                    new JsonJacksonCodec(), totalEventsToRead));

            range(0, totalEventsToRead)
                    .mapToObj(i -> new SampleEvent("" + i, randomUUID()))
                    .parallel()
                    .forEach(baseline::write);
        }

        @SneakyThrows
        @TearDown(Level.Trial)
        public void tearDown() {
            try {
                ((AutoCloseable) baseline).close();
                ((AutoCloseable) redisCacheProvider).close();
            } catch (Exception ignored) {}
        }

    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void readEventsWithoutCache(Blackhole blackhole, Providers providers) {
        readEventsSequentially(blackhole, providers.baseline);
    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void inMemoryCache(Blackhole blackhole, Providers providers) {
        readEventsSequentially(blackhole, providers.inMemoryCacheProvider);
    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void caffeineCache(Blackhole blackhole, Providers providers) {
        readEventsSequentially(blackhole, providers.caffeineCacheProvider);
    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void redisCache(Blackhole blackhole, Providers providers) {
        readEventsSequentially(blackhole, providers.redisCacheProvider);
    }

    private void readEventsSequentially(Blackhole blackhole, StoreProvider provider) {
        try (Stream<Event> stream = provider.readFrom(0)) {
            stream.forEach(blackhole::consume);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        final Options options = new OptionsBuilder().include(CachingProvidersReadBenchmark.class.getSimpleName())
                .detectJvmArgs().jvmArgsAppend("-Xmx2048m")
                .build();
        new Runner(options).run();
    }

}
