package ru.course.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(NamedThreadFactory.class);

    private final String prefix;
    private final boolean daemon;
    private final AtomicInteger counter = new AtomicInteger(1);

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        String threadName = prefix + counter.getAndIncrement();

        Runnable wrapped = () -> {
            try {
                r.run();
            } finally {
                log.info("Thread {} terminated", threadName);
            }
        };

        Thread thread = new Thread(wrapped, threadName);
        thread.setDaemon(daemon);

        log.info("Created thread {}", threadName);
        return thread;
    }
}