package ru.course.threadpool;

public interface RejectionPolicy {
    void reject(Runnable task, CustomThreadPool executor);
}