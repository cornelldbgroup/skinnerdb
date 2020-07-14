package threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Threads pool for joining.parallel execution.
 * Initialize one static thread pool for join phase.
 * Multiple queries can reuse the thread pool
 * without re-initializing it.
 *
 * @author Ziyun Wei
 *
 */
public class ThreadPool {
    /**
     * Thread pool instance for execution.
     */
    public static ExecutorService executorService;
    /**
     * Thread pool instance for post-processing.
     */
    public static ExecutorService postExecutorService;
    /**
     * Initializes a thread pool.
     *
     * @param nrThreads	    Number of threads for join phase.
     */
    public static void initThreadsPool(int nrThreads) {
        if (nrThreads > 0) {
            executorService = Executors.newFixedThreadPool(nrThreads);
            int cores = Runtime.getRuntime().availableProcessors();
            postExecutorService = Executors.newFixedThreadPool(cores);
        }
    }

    public static void close() {
        if (executorService != null)
            executorService.shutdown();
        if (postExecutorService != null)
            postExecutorService.shutdown();;
    }
}
