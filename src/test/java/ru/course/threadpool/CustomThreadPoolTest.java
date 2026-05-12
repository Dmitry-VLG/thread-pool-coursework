package ru.course.threadpool;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomThreadPoolTest {

    @Test
    void submitCallableReturnsValue() throws Exception {
        CustomThreadPool pool = createPool();
        try {
            Future<Integer> future = pool.submit(() -> 42);
            assertEquals(42, future.get(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shutdownRejectsNewTasks() throws Exception {
        CustomThreadPool pool = createPool();
        pool.shutdown();

        assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {
        }));

        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    private CustomThreadPool createPool() {
        return new CustomThreadPool(
                2,
                4,
                2,
                TimeUnit.SECONDS,
                8,
                2,
                1,
                new CallerRunsRejectionPolicy(),
                new NamedThreadFactory("TestPool-worker-", false)
        );
    }
}