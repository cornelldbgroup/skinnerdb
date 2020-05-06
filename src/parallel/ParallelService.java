package parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelService {
    public static ExecutorService POOL;
    public static int POOL_THREADS;


    public static void init() {
        POOL_THREADS = Runtime.getRuntime().availableProcessors();
        POOL =
                Executors.newFixedThreadPool(POOL_THREADS);
    }

    public static void shutdown() {
        POOL.shutdownNow();
    }
}
