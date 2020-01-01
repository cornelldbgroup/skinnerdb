package joining.parallel.parallelization.search;

import config.LoggingConfig;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
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

public class SearchParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<SPJoin> spJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget    the budget per episode
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public SearchParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
        // Initialize UCT join order search tree.
        SPNode root = new SPNode(0, query, true, nrThreads);
        SearchScheduler scheduler = new SearchScheduler(query, root, spJoins, nrThreads);
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicBoolean end = new AtomicBoolean(false);
        int nrTables = query.nrJoined;
        List<SearchTask> tasks = new ArrayList<>();
        // logs list
        List<String>[] logs = new List[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        for (int i = 0; i < nrThreads; i++) {
            SPJoin spJoin = spJoins.get(i);
            if (spJoin.nextActions != null) {
                SearchTask searchTask = new SearchTask(query, context, root, spJoin, scheduler, end);
                tasks.add(searchTask);
            }
        }
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        futures.forEach(futureResult -> {
            try {
                SearchResult result = futureResult.get();
                resultList.addAll(result.result);
                if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                    logs[result.id] = result.logs;
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        });
        JoinStats.nrSamples = 0;
        for (SPJoin joinOp: spJoins) {
            JoinStats.nrSamples = Math.max(joinOp.roundCtr, JoinStats.nrSamples);
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            if (ParallelConfig.PARALLEL_SPEC == 2) {
                LogUtils.writeLogs(logs, "verbose/search/" + QueryStats.queryName);
            }
            else if (ParallelConfig.PARALLEL_SPEC == 3) {
                LogUtils.writeLogs(logs, "verbose/dynamic_search/" + QueryStats.queryName);
            }
        }

        System.out.println("Result Set: " + resultList.size());
    }
}
