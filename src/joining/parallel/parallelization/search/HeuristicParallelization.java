package joining.parallel.parallelization.search;

import config.LoggingConfig;
import config.ParallelConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.ASPNode;
import joining.parallel.uct.HSPNode;
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

/**
 * Partition the search space but join tables
 * on the entire database. Each thread will
 * calculate the partition adaptively during the
 * learning. By doing this, the parallelization doesn't
 * rely on any heuristics.
 *
 * @author Anonymous
 */
public class HeuristicParallelization extends Parallelization {
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
    public HeuristicParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
        HSPNode root = new HSPNode(0, query, true, nrThreads);
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicBoolean end = new AtomicBoolean(false);
        int nrTables = query.nrJoined;
        List<HeuristicSearchTask> tasks = new ArrayList<>();
        // logs list
        List<String>[] logs = new List[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        for (int i = 0; i < nrThreads; i++) {
            SPJoin spJoin = spJoins.get(i);
            HeuristicSearchTask heuristicSearchTask = new HeuristicSearchTask(query, root, spJoin, spJoins, end);
            tasks.add(heuristicSearchTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<SearchResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
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
        JoinStats.nrTuples = 0;
        for (SPJoin joinOp: spJoins) {
            if (joinOp.roundCtr > JoinStats.nrSamples) {
                JoinStats.nrSamples = joinOp.roundCtr;
                JoinStats.nrTuples = joinOp.statsInstance.nrTuples;
            }
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/heuristic_search/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}
