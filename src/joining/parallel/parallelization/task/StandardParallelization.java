package joining.parallel.parallelization.task;

import catalog.CatalogManager;
import config.JoinConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.ParallelConfig;
import joining.join.OldJoin;
import joining.parallel.join.FixJoin;
import joining.parallel.join.SPJoin;
import joining.parallel.join.ThreadResult;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import operators.Materialize;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class StandardParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<FixJoin> spJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public StandardParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
            FixJoin modJoin = new FixJoin(query, context, budget, nrThreads, i, predToEval, 0);
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
        List<DBTask> tasks = new ArrayList<>();
        // logs list
        List<String>[] logs = new List[nrThreads];
        int nrExecutors = Math.min(ParallelConfig.NR_EXECUTORS + 1, nrThreads);
        // best join orders
        int[][] best = new int[nrExecutors][nrTables + 1];
        double[][] probs = new double[nrExecutors][nrTables];
        for (int i = 0; i < nrExecutors; i++) {
            logs[i] = new ArrayList<>();
            best[i][0] = -1;
        }
        for (int i = 0; i < nrExecutors; i++) {
            FixJoin spJoin = spJoins.get(i);
            DBTask dbTask = new DBTask(query, spJoin, end, best, probs, spJoins);
            tasks.add(dbTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<TaskResult>> futures = executorService.invokeAll(tasks);
//        TaskResult result = executorService.invokeAny(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
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
        // close thread pool
        for (FixJoin fixJoin: spJoins) {
            List<int[]>[] opResultList = fixJoin.resultList;
            ThreadResult[][] threadResultsList = fixJoin.threadResultsList;
            if (opResultList != null) {
                for (List<int[]> results: opResultList) {
                    if (results != null) {
                        for (int[] tuples: results) {
                            resultList.add(new ResultTuple(tuples));
                        }
                    }
                }
            }
            if (fixJoin.threadResultsList != null) {
                for (ThreadResult[] results: threadResultsList) {
                    if (results != null) {
                        for (ThreadResult threadTuples: results) {
                            int count = threadTuples.count;
                            int[] tuples = new int[nrTables];
                            for (int i = 0; i < count; i++) {
                                int startPos = i * nrTables + 1;
                                System.arraycopy(threadTuples.result, startPos, tuples, 0, nrTables);
                                resultList.add(new ResultTuple(tuples));
                            }
                        }
                    }
                }
            }
            if (fixJoin.executorService != null)
                fixJoin.executorService.shutdown();
        }
        long nrSamples = 0;
        long nrTuples = 0;
        for (SPJoin joinOp: spJoins) {
            nrSamples = Math.max(joinOp.roundCtr, nrSamples);
            nrTuples = Math.max(joinOp.statsInstance.nrTuples, nrTuples);
        }
        JoinStats.nrSamples = nrSamples;
        JoinStats.nrTuples = nrTuples;


        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/task/" + QueryStats.queryName);
        }

        System.out.println("Result Set: " + resultList.size());
    }
}
