package joining.parallel.parallelization.task;

import config.LoggingConfig;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.search.SearchResult;
import joining.parallel.parallelization.search.SearchScheduler;
import joining.parallel.parallelization.search.SearchTask;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.UctNode;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<SPJoin> spJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public TaskParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        int nrTables = query.nrJoined;
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, 1);
        for (int i = 0; i < nrThreads; i++) {
            SubJoin modJoin = new SubJoin(query, context, budget, nrThreads, i, predToEval);
            modJoin.tracker = tracker;
            spJoins.add(modJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicBoolean end = new AtomicBoolean(false);
        int nrTables = query.nrJoined;
        List<ExecutorTask> tasks = new ArrayList<>();
        // logs list
        List<String>[] logs = new List[nrThreads];
        // best join orders
        int[][] best = new int[nrThreads][nrTables + 1];

        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        for (int i = 0; i < nrThreads; i++) {
            SPJoin spJoin = spJoins.get(i);
            ExecutorTask executorTask = new ExecutorTask(query, spJoin, end, best);
            tasks.add(executorTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<TaskResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        futures.forEach(futureResult -> {
            try {
                TaskResult result = futureResult.get();
                resultList.addAll(result.result);
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    logs[result.id] = result.logs;
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        });

        long nrSamples = 0;
        for (SPJoin joinOp: spJoins) {
            nrSamples = Math.max(joinOp.roundCtr, nrSamples);
            JoinStats.nrTuples += joinOp.statsInstance.nrTuples;
        }
        JoinStats.nrSamples = nrSamples;

        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/task/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}
