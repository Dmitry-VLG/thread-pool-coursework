package ru.course.threadpool;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface CustomExecutor extends Executor {

    <T> Future<T> submit(Callable<T> task);

    Future<?> submit(Runnable task);

    void shutdown();

    List<Runnable> shutdownNow();

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    boolean isShutdown();

    boolean isTerminated();
}