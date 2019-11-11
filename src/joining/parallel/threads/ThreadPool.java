package joining.parallel.threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Threads pool for joining.parallel execution.
 *
 * @author Ziyun Wei
 *
 */
public class ThreadPool {
    /**
     * Thread pool instance.
     */
    public static ExecutorService executorService;
    /**
     * Initializes a thread pool.
     *
     * @param nrThreads	    Number of threads.
     */
    public static void initThreadsPool(int nrThreads) {
        executorService = Executors.newFixedThreadPool(nrThreads);
    }

    public static void close() {
        executorService.shutdown();
    }
}
