package joining.parallel.threads;

import net.openhft.affinity.AffinityThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.openhft.affinity.AffinityStrategies.*;

/**
 * Threads pool for joining.parallel execution.
 *
 * @author Anonymous
 *
 */
public class ThreadPool {
    /**
     * Thread pool instance for execution.
     */
    public static ExecutorService executorService;
    /**
     * Thread pool instance for preprocessing.
     */
    public static ExecutorService preprocessingService;
    /**
     * Initializes a thread pool.
     *
     * @param nrThreads	    Number of threads.
     */
    public static void initThreadsPool(int nrThreads, int preThreads) {
        if (nrThreads > 0) {
            executorService = Executors.newFixedThreadPool(nrThreads);
//            executorService = Executors.newFixedThreadPool(nrThreads,
//                    new AffinityThreadFactory("bg", SAME_CORE, DIFFERENT_SOCKET, ANY));
        }
//        if (preThreads > 0) {
//            preprocessingService = Executors.newFixedThreadPool(preThreads);
//        }
    }

    public static void close() {
        if (executorService != null)
            executorService.shutdown();
        if (preprocessingService != null)
            preprocessingService.shutdown();
    }
}
