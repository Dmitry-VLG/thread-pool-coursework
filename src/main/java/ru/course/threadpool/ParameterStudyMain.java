package ru.course.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ParameterStudyMain {

    private static volatile long blackhole;

    public static void main(String[] args) throws Exception {
        List<Config> configs = List.of(
                new Config("A", 2, 4, 100, 2, 0, 2),
                new Config("B", 4, 8, 1000, 3, 1, 2),
                new Config("C", 6, 10, 2000, 5, 2, 4)
        );

        System.out.println("| Конфигурация | core | max | queueSize | keepAlive | minSpare | queues | total time, ms | throughput, tasks/s | Комментарий |");
        System.out.println("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|");

        for (Config config : configs) {
            Result result = runConfig(config, 5000);
            System.out.printf(
                    "| %s | %d | %d | %d | %d | %d | %d | %.2f | %.2f | %s |%n",
                    config.name,
                    config.core,
                    config.max,
                    config.queueSize,
                    config.keepAliveSeconds,
                    config.minSpareThreads,
                    config.queueCount,
                    result.totalTimeMs,
                    result.throughput,
                    result.comment
            );
        }
    }

    private static Result runConfig(Config config, int tasks) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                config.core,
                config.max,
                config.keepAliveSeconds,
                TimeUnit.SECONDS,
                config.queueSize,
                config.queueCount,
                config.minSpareThreads,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("study-" + config.name + "-", false)
        );

        CountDownLatch latch = new CountDownLatch(tasks);

        long start = System.nanoTime();

        for (int i = 0; i < tasks; i++) {
            pool.execute(() -> {
                try {
                    cpuWorkload();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long end = System.nanoTime();

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.MINUTES);

        double totalTimeMs = (end - start) / 1_000_000.0;
        double throughput = tasks / ((end - start) / 1_000_000_000.0);

        String comment;
        if ("A".equals(config.name)) {
            comment = "Меньше потоков, быстрее насыщение";
        } else if ("B".equals(config.name)) {
            comment = "Сбалансированная конфигурация";
        } else {
            comment = "Больше ресурсов, выше стоимость простоя";
        }

        return new Result(totalTimeMs, throughput, comment);
    }

    private static void cpuWorkload() {
        long acc = 0;
        for (int i = 0; i < 100_000; i++) {
            acc += (long) i * i;
        }
        blackhole = acc;
    }

    private static final class Config {
        private final String name;
        private final int core;
        private final int max;
        private final int queueSize;
        private final int keepAliveSeconds;
        private final int minSpareThreads;
        private final int queueCount;

        private Config(String name, int core, int max, int queueSize, int keepAliveSeconds, int minSpareThreads, int queueCount) {
            this.name = name;
            this.core = core;
            this.max = max;
            this.queueSize = queueSize;
            this.keepAliveSeconds = keepAliveSeconds;
            this.minSpareThreads = minSpareThreads;
            this.queueCount = queueCount;
        }
    }

    private static final class Result {
        private final double totalTimeMs;
        private final double throughput;
        private final String comment;

        private Result(double totalTimeMs, double throughput, String comment) {
            this.totalTimeMs = totalTimeMs;
            this.throughput = throughput;
            this.comment = comment;
        }
    }
}