package parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelService {
    public static ExecutorService HIGH_POOL;
    public static ExecutorService LOW_POOL;

    public static void init() {
        int threads = Runtime.getRuntime().availableProcessors();
        HIGH_POOL =
                Executors.newFixedThreadPool(threads - 1);
        LOW_POOL = Executors.newFixedThreadPool(1);
    }

    public static void shutdown() {
        HIGH_POOL.shutdownNow();
        LOW_POOL.shutdownNow();
    }
}
