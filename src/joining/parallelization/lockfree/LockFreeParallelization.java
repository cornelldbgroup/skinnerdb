package joining.parallelization.lockfree;

import config.JoinConfig;
import config.LoggingConfig;
import joining.ParallelJoinProcessor;
import joining.join.DPJoin;
import joining.join.ModJoin;
import joining.parallelization.EndPlan;
import joining.parallelization.Parallelization;
import joining.progress.ParallelProgressTracker;
import joining.progress.ProgressTracker;
import joining.result.ResultTuple;
import joining.uct.DPNode;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import threads.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    public List<DPJoin> dpJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public LockFreeParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Initialize multi-way join operator
        ParallelProgressTracker tracker = new ParallelProgressTracker(query.nrJoined, nrThreads);
//        ProgressTracker oldtracker = new ProgressTracker(query.nrJoined, );
        for (int i = 0; i < nrThreads; i++) {
            ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i);
            modJoin.tracker = tracker;
            dpJoins.add(modJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Initialize UCT join order search tree.
        DPNode root = new DPNode(0, query, true, nrThreads);
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Initialize variables for broadcasting.
        int nrTables = query.nrJoined;
        EndPlan endPlan = new EndPlan(nrTables);
        List<LockFreeTask> tasks = new ArrayList<>();
        // Mutex shared by multiple threads.
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean end = new AtomicBoolean(false);
        // logs list
        List<String>[] logs = new List[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        for (int i = 0; i < nrThreads; i++) {
            LockFreeTask lockFreeTask = new LockFreeTask(query, context, root, endPlan, end, lock, dpJoins.get(i));
            tasks.add(lockFreeTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<LockFreeResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.time = executionEnd - executionStart;
        futures.forEach(futureResult -> {
            try {
                LockFreeResult result = futureResult.get();
                resultList.addAll(result.result);
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    logs[result.id] = result.logs;
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        });
        JoinStats.nrSamples = 0;
        for (DPJoin joinOp: dpJoins) {
            JoinStats.nrSamples = Math.max(joinOp.roundCtr, JoinStats.nrSamples);
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            writeLogs(logs, "verbose/lockFree/33c");
        }
    }
}
