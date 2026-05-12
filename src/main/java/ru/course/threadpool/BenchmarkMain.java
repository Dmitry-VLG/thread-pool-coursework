package ru.course.threadpool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BenchmarkMain {

    private static volatile long blackhole;

    public static void main(String[] args) throws Exception {
        int core = 4;
        int max = 8;
        int queueSize = 1000;
        int queueCount = 2;
        int minSpareThreads = 1;
        long keepAliveSeconds = 3;

        System.out.println("=== PERFORMANCE BENCHMARK ===");
        System.out.println("Warmup: 1 run");
        System.out.println("Measured: 3 runs");
        System.out.println();

        BenchmarkResult customCpu = averageOfRuns(
                3,
                () -> runCustomBenchmark(
                        "CPU-bound",
                        core,
                        max,
                        queueSize,
                        queueCount,
                        minSpareThreads,
                        keepAliveSeconds,
                        5000,
                        BenchmarkMain::cpuWorkload
                )
        );

        BenchmarkResult jdkCpu = averageOfRuns(
                3,
                () -> runJdkBenchmark(
                        "CPU-bound",
                        core,
                        max,
                        queueSize,
                        keepAliveSeconds,
                        5000,
                        BenchmarkMain::cpuWorkload
                )
        );

        BenchmarkResult customSleep = averageOfRuns(
                3,
                () -> runCustomBenchmark(
                        "Sleep/I/O-like",
                        core,
                        max,
                        queueSize,
                        queueCount,
                        minSpareThreads,
                        keepAliveSeconds,
                        1000,
                        BenchmarkMain::sleepWorkload
                )
        );

        BenchmarkResult jdkSleep = averageOfRuns(
                3,
                () -> runJdkBenchmark(
                        "Sleep/I/O-like",
                        core,
                        max,
                        queueSize,
                        keepAliveSeconds,
                        1000,
                        BenchmarkMain::sleepWorkload
                )
        );

        System.out.println("### Результаты");
        System.out.println();
        System.out.println("| Сценарий | Пул | core/max | queueSize | tasks | total time, ms | throughput, tasks/s | avg latency, ms | p95 latency, ms |");
        System.out.println("|---|---|---:|---:|---:|---:|---:|---:|---:|");
        printMarkdownRow(customCpu);
        printMarkdownRow(jdkCpu);
        printMarkdownRow(customSleep);
        printMarkdownRow(jdkSleep);
    }

    private static BenchmarkResult runCustomBenchmark(String scenario,
                                                      int core,
                                                      int max,
                                                      int queueSize,
                                                      int queueCount,
                                                      int minSpareThreads,
                                                      long keepAliveSeconds,
                                                      int tasks,
                                                      TaskWorkload workload) throws Exception {

        warmupCustom(core, max, queueSize, queueCount, minSpareThreads, keepAliveSeconds, workload);

        CustomThreadPool pool = new CustomThreadPool(
                core,
                max,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                queueSize,
                queueCount,
                minSpareThreads,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("bench-custom-", false)
        );

        BenchmarkResult result = runMeasured(
                scenario,
                "CustomThreadPool",
                core,
                max,
                queueSize,
                tasks,
                workload,
                pool::execute
        );

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.MINUTES);

        return result;
    }

    private static BenchmarkResult runJdkBenchmark(String scenario,
                                                   int core,
                                                   int max,
                                                   int queueSize,
                                                   long keepAliveSeconds,
                                                   int tasks,
                                                   TaskWorkload workload) throws Exception {

        warmupJdk(core, max, queueSize, keepAliveSeconds, workload);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                core,
                max,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        executor.prestartAllCoreThreads();

        BenchmarkResult result = runMeasured(
                scenario,
                "ThreadPoolExecutor",
                core,
                max,
                queueSize,
                tasks,
                workload,
                executor::execute
        );

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        return result;
    }

    private static BenchmarkResult runMeasured(String scenario,
                                               String poolName,
                                               int core,
                                               int max,
                                               int queueSize,
                                               int tasks,
                                               TaskWorkload workload,
                                               TaskSubmitter submitter) throws Exception {

        CountDownLatch latch = new CountDownLatch(tasks);
        long[] latenciesNanos = new long[tasks];

        long benchmarkStart = System.nanoTime();

        for (int i = 0; i < tasks; i++) {
            final int taskIndex = i;
            final long submittedAt = System.nanoTime();

            submitter.submit(() -> {
                try {
                    workload.run();
                } finally {
                    latenciesNanos[taskIndex] = System.nanoTime() - submittedAt;
                    latch.countDown();
                }
            });
        }

        latch.await();
        long benchmarkEnd = System.nanoTime();

        double totalTimeMs = nanosToMillis(benchmarkEnd - benchmarkStart);
        double throughput = tasks / ((benchmarkEnd - benchmarkStart) / 1_000_000_000.0);
        double avgLatencyMs = averageLatencyMs(latenciesNanos);
        double p95LatencyMs = percentileMs(latenciesNanos, 95);

        return new BenchmarkResult(
                scenario,
                poolName,
                core,
                max,
                queueSize,
                tasks,
                totalTimeMs,
                throughput,
                avgLatencyMs,
                p95LatencyMs
        );
    }

    private static void warmupCustom(int core,
                                     int max,
                                     int queueSize,
                                     int queueCount,
                                     int minSpareThreads,
                                     long keepAliveSeconds,
                                     TaskWorkload workload) throws Exception {

        CustomThreadPool pool = new CustomThreadPool(
                core,
                max,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                queueSize,
                queueCount,
                minSpareThreads,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("warmup-custom-", false)
        );

        CountDownLatch latch = new CountDownLatch(300);
        for (int i = 0; i < 300; i++) {
            pool.execute(() -> {
                try {
                    workload.run();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.MINUTES);
    }

    private static void warmupJdk(int core,
                                  int max,
                                  int queueSize,
                                  long keepAliveSeconds,
                                  TaskWorkload workload) throws Exception {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                core,
                max,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        executor.prestartAllCoreThreads();

        CountDownLatch latch = new CountDownLatch(300);
        for (int i = 0; i < 300; i++) {
            executor.execute(() -> {
                try {
                    workload.run();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);
    }

    private static BenchmarkResult averageOfRuns(int runs, BenchmarkSupplier supplier) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        for (int i = 0; i < runs; i++) {
            results.add(supplier.get());
        }

        BenchmarkResult first = results.get(0);

        double totalTimeMs = results.stream().mapToDouble(r -> r.totalTimeMs).average().orElse(0.0);
        double throughput = results.stream().mapToDouble(r -> r.throughput).average().orElse(0.0);
        double avgLatencyMs = results.stream().mapToDouble(r -> r.avgLatencyMs).average().orElse(0.0);
        double p95LatencyMs = results.stream().mapToDouble(r -> r.p95LatencyMs).average().orElse(0.0);

        return new BenchmarkResult(
                first.scenario,
                first.poolName,
                first.core,
                first.max,
                first.queueSize,
                first.tasks,
                totalTimeMs,
                throughput,
                avgLatencyMs,
                p95LatencyMs
        );
    }

    private static void cpuWorkload() {
        long acc = 0;
        for (int i = 0; i < 100_000; i++) {
            acc += (long) i * i;
        }
        blackhole = acc;
    }

    private static void sleepWorkload() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static double averageLatencyMs(long[] latenciesNanos) {
        long sum = 0;
        for (long latency : latenciesNanos) {
            sum += latency;
        }
        return nanosToMillis(sum) / latenciesNanos.length;
    }

    private static double percentileMs(long[] latenciesNanos, int percentile) {
        long[] copy = Arrays.copyOf(latenciesNanos, latenciesNanos.length);
        Arrays.sort(copy);

        int index = (int) Math.ceil((percentile / 100.0) * copy.length) - 1;
        index = Math.max(0, Math.min(index, copy.length - 1));

        return nanosToMillis(copy[index]);
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static void printMarkdownRow(BenchmarkResult r) {
        System.out.printf(
                "| %s | %s | %d/%d | %d | %d | %.2f | %.2f | %.2f | %.2f |%n",
                r.scenario,
                r.poolName,
                r.core,
                r.max,
                r.queueSize,
                r.tasks,
                r.totalTimeMs,
                r.throughput,
                r.avgLatencyMs,
                r.p95LatencyMs
        );
    }

    @FunctionalInterface
    private interface TaskSubmitter {
        void submit(Runnable task);
    }

    @FunctionalInterface
    private interface TaskWorkload {
        void run();
    }

    @FunctionalInterface
    private interface BenchmarkSupplier {
        BenchmarkResult get() throws Exception;
    }

    private static final class BenchmarkResult {
        private final String scenario;
        private final String poolName;
        private final int core;
        private final int max;
        private final int queueSize;
        private final int tasks;
        private final double totalTimeMs;
        private final double throughput;
        private final double avgLatencyMs;
        private final double p95LatencyMs;

        private BenchmarkResult(String scenario,
                                String poolName,
                                int core,
                                int max,
                                int queueSize,
                                int tasks,
                                double totalTimeMs,
                                double throughput,
                                double avgLatencyMs,
                                double p95LatencyMs) {
            this.scenario = scenario;
            this.poolName = poolName;
            this.core = core;
            this.max = max;
            this.queueSize = queueSize;
            this.tasks = tasks;
            this.totalTimeMs = totalTimeMs;
            this.throughput = throughput;
            this.avgLatencyMs = avgLatencyMs;
            this.p95LatencyMs = p95LatencyMs;
        }
    }
}