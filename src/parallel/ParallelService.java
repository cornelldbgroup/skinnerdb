package parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelService {
    public static ExecutorService HIGH_POOL;
    public static int HIGH_POOL_THREADS;


    public static void init() {
        HIGH_POOL_THREADS = Runtime.getRuntime().availableProcessors();
        HIGH_POOL =
                Executors.newFixedThreadPool(HIGH_POOL_THREADS);
    }

    public static void shutdown() {
        HIGH_POOL.shutdownNow();
    }
}
