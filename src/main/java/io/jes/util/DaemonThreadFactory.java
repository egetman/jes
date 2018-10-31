package io.jes.util;

import java.text.MessageFormat;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

public class DaemonThreadFactory implements ThreadFactory {

    private static final Thread.UncaughtExceptionHandler TRACE_PRINTER = new TracePrinter();

    private final String threadName;
    private final AtomicLong workerNumber = new AtomicLong();

    public DaemonThreadFactory(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public Thread newThread(@Nonnull Runnable runnable) {
        String name = MessageFormat.format("{0} [{1}]", threadName, workerNumber.getAndIncrement());
        Thread thread = new Thread(runnable, name);
        thread.setUncaughtExceptionHandler(TRACE_PRINTER);
        thread.setDaemon(true);
        return thread;
    }

    @Slf4j
    private static class TracePrinter implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread thread, Throwable ex) {
            log.error("Uncaught exception for {}:", thread.getName(), ex);
        }
    }

}
