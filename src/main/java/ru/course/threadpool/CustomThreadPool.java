package ru.course.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CustomThreadPool implements CustomExecutor {

    private static final Logger log = LoggerFactory.getLogger(CustomThreadPool.class);

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTimeMillis;
    private final int queueSize;
    private final int minSpareThreads;
    private final int queueCount;

    private final RejectionPolicy rejectionPolicy;
    private final NamedThreadFactory threadFactory;

    private final List<BlockingQueue<Runnable>> queues;
    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();
    private final List<Runnable> lastCancelledTasks = Collections.synchronizedList(new ArrayList<>());

    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final AtomicInteger idleWorkers = new AtomicInteger(0);
    private final ReentrantLock mainLock = new ReentrantLock();

    private volatile boolean shutdown;
    private volatile boolean stop;

    public CustomThreadPool(int corePoolSize,
                            int maxPoolSize,
                            long keepAliveTime,
                            TimeUnit unit,
                            int queueSize,
                            int queueCount,
                            int minSpareThreads,
                            RejectionPolicy rejectionPolicy,
                            NamedThreadFactory threadFactory) {

        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("corePoolSize must be > 0");
        }
        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
        }
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be > 0");
        }
        if (queueCount <= 0) {
            throw new IllegalArgumentException("queueCount must be > 0");
        }
        if (minSpareThreads < 0) {
            throw new IllegalArgumentException("minSpareThreads must be >= 0");
        }

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTimeMillis = unit.toMillis(keepAliveTime);
        this.queueSize = queueSize;
        this.queueCount = queueCount;
        this.minSpareThreads = minSpareThreads;
        this.rejectionPolicy = Objects.requireNonNull(rejectionPolicy, "rejectionPolicy must not be null");
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory must not be null");

        this.queues = new ArrayList<>(queueCount);
        for (int i = 0; i < queueCount; i++) {
            this.queues.add(new ArrayBlockingQueue<>(queueSize));
        }

        prestartCoreWorkers();
    }

    private void prestartCoreWorkers() {
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(i % queueCount, true);
        }
    }

    private int nextQueueIndex() {
        return Math.floorMod(roundRobinIndex.getAndIncrement(), queueCount);
    }

    private boolean offerTask(Runnable task, int preferredQueue) {
        if (queues.get(preferredQueue).offer(task)) {
            log.debug("Task accepted into queue {}", preferredQueue);
            return true;
        }

        for (int i = 1; i < queueCount; i++) {
            int anotherQueue = (preferredQueue + i) % queueCount;
            if (queues.get(anotherQueue).offer(task)) {
                log.debug("Preferred queue {} is full, task redirected to queue {}", preferredQueue, anotherQueue);
                return true;
            }
        }

        return false;
    }

    private void ensureSpareWorkers() {
        if (!shutdown && idleWorkers.get() < minSpareThreads) {
            addWorker(nextQueueIndex(), false);
        }
    }

    private boolean addWorker(int queueIndex, boolean core) {
        mainLock.lock();
        try {
            if (stop) {
                return false;
            }

            if (shutdown && !core) {
                return false;
            }

            int limit = core ? corePoolSize : maxPoolSize;
            if (workers.size() >= limit) {
                return false;
            }

            Worker worker = new Worker(queueIndex, core);
            Thread thread = threadFactory.newThread(worker);
            worker.setThread(thread);

            workers.add(worker);
            thread.start();

            log.info("Started {} worker {} for queue {}",
                    core ? "core" : "extra",
                    thread.getName(),
                    queueIndex);

            return true;
        } finally {
            mainLock.unlock();
        }
    }

    private void onWorkerExit(Worker worker) {
        mainLock.lock();
        try {
            workers.remove(worker);
            log.info("Worker {} removed. Active workers: {}", worker.threadName(), workers.size());

            if (!shutdown && workers.size() < corePoolSize) {
                addWorker(worker.homeQueueIndex, true);
            }
        } finally {
            mainLock.unlock();
        }
    }

    private Runnable stealFromOtherQueues(int homeQueueIndex) {
        for (int i = 1; i < queueCount; i++) {
            int anotherQueue = (homeQueueIndex + i) % queueCount;
            Runnable task = queues.get(anotherQueue).poll();
            if (task != null) {
                log.debug("{} stole task from queue {}", Thread.currentThread().getName(), anotherQueue);
                return task;
            }
        }
        return null;
    }

    private Runnable getTask(int homeQueueIndex, boolean core) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(keepAliveTimeMillis);
        idleWorkers.incrementAndGet();
        try {
            while (true) {
                if (stop) {
                    return null;
                }

                Runnable task = queues.get(homeQueueIndex).poll(200, TimeUnit.MILLISECONDS);
                if (task != null) {
                    return task;
                }

                task = stealFromOtherQueues(homeQueueIndex);
                if (task != null) {
                    return task;
                }

                if (shutdown && allQueuesEmpty()) {
                    return null;
                }

                if (!core && System.nanoTime() >= deadline) {
                    log.info("Worker {} reached keepAlive timeout and will stop",
                            Thread.currentThread().getName());
                    return null;
                }
            }
        } finally {
            idleWorkers.decrementAndGet();
        }
    }

    private boolean allQueuesEmpty() {
        for (BlockingQueue<Runnable> queue : queues) {
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command must not be null");

        if (shutdown) {
            throw new RejectedExecutionException("Thread pool is shutting down");
        }

        int preferredQueue = nextQueueIndex();

        if (offerTask(command, preferredQueue)) {
            ensureSpareWorkers();
            return;
        }

        if (addWorker(preferredQueue, false) && offerTask(command, preferredQueue)) {
            return;
        }

        rejectionPolicy.reject(command, this);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        Objects.requireNonNull(task, "task must not be null");
        FutureTask<T> futureTask = new FutureTask<>(task);
        execute(futureTask);
        return futureTask;
    }

    public Future<?> submit(Runnable task) {
        Objects.requireNonNull(task, "task must not be null");
        FutureTask<?> futureTask = new FutureTask<>(task, null);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            if (shutdown) {
                return;
            }
            shutdown = true;
            log.info("Shutdown requested");
        } finally {
            mainLock.unlock();
        }

        for (Worker worker : workers) {
            worker.interruptIfNeeded();
        }
    }

    @Override
    public void shutdownNow() {
        mainLock.lock();
        try {
            shutdown = true;
            stop = true;
            log.warn("ShutdownNow requested");

            lastCancelledTasks.clear();
            for (BlockingQueue<Runnable> queue : queues) {
                queue.drainTo(lastCancelledTasks);
            }
        } finally {
            mainLock.unlock();
        }

        for (Worker worker : workers) {
            worker.interruptIfNeeded();
        }
    }

    public List<Runnable> getLastCancelledTasks() {
        synchronized (lastCancelledTasks) {
            return new ArrayList<>(lastCancelledTasks);
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        while (System.nanoTime() < deadline) {
            if (isTerminated()) {
                return true;
            }
            Thread.sleep(50);
        }

        return isTerminated();
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isTerminated() {
        return shutdown && workers.isEmpty() && allQueuesEmpty();
    }

    public int getWorkerCount() {
        return workers.size();
    }

    public int getIdleWorkers() {
        return idleWorkers.get();
    }

    public int getQueueCount() {
        return queueCount;
    }

    public int getQueuedTaskCount() {
        int result = 0;
        for (BlockingQueue<Runnable> queue : queues) {
            result += queue.size();
        }
        return result;
    }

    private final class Worker implements Runnable {
        private final int homeQueueIndex;
        private final boolean core;
        private Thread thread;

        private Worker(int homeQueueIndex, boolean core) {
            this.homeQueueIndex = homeQueueIndex;
            this.core = core;
        }

        private void setThread(Thread thread) {
            this.thread = thread;
        }

        private String threadName() {
            return thread == null ? "unknown" : thread.getName();
        }

        private void interruptIfNeeded() {
            if (thread != null) {
                thread.interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Runnable task;

                    try {
                        task = getTask(homeQueueIndex, core);
                    } catch (InterruptedException e) {
                        if (stop || (shutdown && allQueuesEmpty())) {
                            break;
                        }
                        log.debug("Worker {} interrupted but continues waiting",
                                Thread.currentThread().getName());
                        continue;
                    }

                    if (task == null) {
                        break;
                    }

                    if (stop) {
                        log.debug("Worker {} will not start a new task because pool is in STOP state",
                                Thread.currentThread().getName());
                        break;
                    }

                    // shutdown() запрещает только прием новых задач.
                    // Уже принятые задачи пул должен корректно доработать.
                    try {
                        log.debug("Worker {} executes task", Thread.currentThread().getName());
                        task.run();
                    } catch (Throwable t) {
                        log.error("Task failed in worker {}", Thread.currentThread().getName(), t);
                    }
                }
            } finally {
                log.info("Worker {} stopped", Thread.currentThread().getName());
                onWorkerExit(this);
            }
        }
    }
}