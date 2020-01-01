package joining.parallel.parallelization.tree;

import config.LoggingConfig;
import joining.parallel.join.SPJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.DPNode;
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

public class TreeParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<SPJoin> spJoins = new ArrayList<>(nrThreads);
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public TreeParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
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
            SubJoin spJoin = new SubJoin(query, context, budget, nrThreads, i, predToEval);
            spJoin.tracker = tracker;
            spJoins.add(spJoin);
        }
    }

    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        // Initialize UCT join order search tree.
        SPNode root = new SPNode(0, query, true, nrThreads);
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicBoolean end = new AtomicBoolean(false);
        List<TreeTask> tasks = new ArrayList<>();
        // logs list
        List<String>[] logs = new List[nrThreads];
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            for (int i = 0; i < nrThreads; i++) {
                logs[i] = new ArrayList<>();
            }
        }
        // initialize tasks for threads.
        for (int i = 0; i < nrThreads; i++) {
            SPJoin spJoin = spJoins.get(i);
            TreeTask rootTask = new TreeTask(query, context, root, spJoin, end);
            tasks.add(rootTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<TreeResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
        // inserting tuples into a result list
        for (int i = 0; i < nrThreads; i++) {
            Future<TreeResult> futureResult = futures.get(i);
            try {
                TreeResult result = futureResult.get();
                resultList.addAll(result.result);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        JoinStats.nrSamples = 0;
        for (SPJoin joinOp: spJoins) {
            JoinStats.nrSamples = Math.max(joinOp.roundCtr, JoinStats.nrSamples);
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            LogUtils.writeLogs(logs, "verbose/tree/" + QueryStats.queryName);
        }
        System.out.println("Result Set: " + resultList.size());
    }
}
