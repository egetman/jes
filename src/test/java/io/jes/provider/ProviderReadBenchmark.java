package io.jes.provider;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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

import io.jes.Event;
import io.jes.internal.Events;
import io.jes.provider.cache.RedisCacheProvider;
import lombok.SneakyThrows;

import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;

@Fork(1)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("DefaultAnnotationParam")
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class ProviderReadBenchmark {

    @SuppressWarnings("FieldCanBeLocal")
    private int size = 5000;
    private StoreProvider provider;

    @Setup(Level.Trial)
    public void setUp() {
        provider = new JdbcStoreProvider<>(newPostgresDataSource("sample"), String.class);
        //provider = new CachingStoreProvider(provider, new InMemoryCache());
        provider = new CachingStoreProvider(provider, new RedisCacheProvider(newRedissonClient()));
        //provider = new InMemoryStoreProvider();
        IntStream.range(0, size).mapToObj(i -> new Events.SampleEvent("" + i, UUID.randomUUID()))
                .parallel()
                .forEach(provider::write);
    }

    @SneakyThrows
    @TearDown(Level.Trial)
    public void tearDown() {
        if (provider instanceof AutoCloseable) {
            ((AutoCloseable) provider).close();
        }
    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void sequentialRead(Blackhole blackhole) {
        readEventsSequentially(blackhole);
    }

    @Benchmark
    @SneakyThrows
    @SuppressWarnings("unused")
    public void repeatableSequentialRead(Blackhole blackhole) {
        readEventsSequentially(blackhole);
        readEventsSequentially(blackhole);
    }

    private void readEventsSequentially(Blackhole blackhole) {
        try (Stream<Event> stream = provider.readFrom(0)) {
            stream.forEach(blackhole::consume);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        final Options options = new OptionsBuilder().include(ProviderReadBenchmark.class.getSimpleName())
                .detectJvmArgs().jvmArgsAppend("-Xmx1024m")
                .build();
        new Runner(options).run();
    }

}