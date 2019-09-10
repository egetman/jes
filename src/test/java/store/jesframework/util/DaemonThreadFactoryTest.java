package store.jesframework.util;

import java.util.concurrent.ThreadFactory;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DaemonThreadFactoryTest {

    @Test
    void shouldProduceDaemonThreadWithSpecifiedName() {
        final ThreadFactory factory = new DaemonThreadFactory("FOO");
        final Thread thread = factory.newThread(() -> System.out.println("BAR"));
        assertTrue(thread.isDaemon());
        assertEquals("FOO [0]", thread.getName());
        assertNotNull(thread.getUncaughtExceptionHandler());
    }

}