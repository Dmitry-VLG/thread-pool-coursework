package ru.course.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                3,
                TimeUnit.SECONDS,
                4,
                2,
                1,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("MyPool-worker-", false)
        );

        log.info("Pool created. workers={}, queues={}", pool.getWorkerCount(), pool.getQueueCount());

        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 1; i <= 8; i++) {
            int taskNumber = i;
            Future<Integer> future = pool.submit(() -> {
                log.info("Callable task {} started", taskNumber);
                Thread.sleep(500);
                log.info("Callable task {} finished", taskNumber);
                return taskNumber * taskNumber;
            });
            futures.add(future);
        }

        pool.submit(() -> log.info("Runnable task without result executed"));

        for (Future<Integer> future : futures) {
            Integer value = future.get();
            log.info("Future result = {}", value);
        }

        log.info("Submitting overload batch");
        for (int i = 1; i <= 20; i++) {
            int taskNumber = i;
            pool.execute(() -> {
                log.info("Overload task {} started in {}", taskNumber, Thread.currentThread().getName());
                sleep(700);
                log.info("Overload task {} finished in {}", taskNumber, Thread.currentThread().getName());
            });
        }

        pool.shutdown();
        boolean terminated = pool.awaitTermination(1, TimeUnit.MINUTES);
        log.info("First pool terminated = {}", terminated);

        log.info("Starting second pool for shutdownNow demo");

        CustomThreadPool secondPool = new CustomThreadPool(
                2,
                3,
                3,
                TimeUnit.SECONDS,
                10,
                2,
                1,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("StopPool-worker-", false)
        );

        for (int i = 1; i <= 10; i++) {
            int taskNumber = i;
            secondPool.execute(() -> {
                log.info("Long task {} started", taskNumber);
                try {
                    Thread.sleep(3000);
                    log.info("Long task {} finished", taskNumber);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("Long task {} interrupted", taskNumber);
                }
            });
        }

        Thread.sleep(1000);

        secondPool.shutdownNow();
        List<Runnable> cancelled = secondPool.getLastCancelledTasks();
        log.info("shutdownNow returned {} not executed tasks", cancelled.size());

        secondPool.awaitTermination(30, TimeUnit.SECONDS);
        log.info("Second pool terminated = {}", secondPool.isTerminated());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}