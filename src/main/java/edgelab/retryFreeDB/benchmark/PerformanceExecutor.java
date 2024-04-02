package edgelab.retryFreeDB.benchmark;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class PerformanceExecutor {
    long timeout;
    public PerformanceExecutor(long timeout) {
        this.timeout = timeout;

    }

    public CompletableFuture<Void> submitToExecute(Runnable task, Runnable onTimeout, Runnable onComplete) throws ExecutionException, InterruptedException, TimeoutException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(task, executor);
        completableFuture.orTimeout(this.timeout, TimeUnit.MILLISECONDS);
        completableFuture.exceptionally(throwable -> {
            completableFuture.cancel(true);
            executor.shutdownNow();
            onTimeout.run();
            return null;
        });
        completableFuture.thenApply(result -> {
            onComplete.run();
            completableFuture.cancel(true);
            executor.shutdownNow();
            return null;
        });
        return completableFuture;
    }

    public void close() {
    }


}
