package ru.course.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;

public class CallerRunsRejectionPolicy implements RejectionPolicy {

    private static final Logger log = LoggerFactory.getLogger(CallerRunsRejectionPolicy.class);

    @Override
    public void reject(Runnable task, CustomThreadPool executor) {
        if (executor.isShutdown()) {
            throw new RejectedExecutionException("Thread pool is already shutdown");
        }

        log.warn("Pool overloaded. Running task in caller thread.");
        task.run();
    }
}