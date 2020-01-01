package joining.parallel.parallelization.lockfree;

import config.LoggingConfig;
import expressions.compilation.KnaryBoolEval;
import joining.result.ResultTuple;
import joining.parallel.join.DPJoin;
import joining.parallel.join.ModJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.DPNode;
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
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeParallelization extends Parallelization {
    /**
     * Multiple join operators for threads
     */
    private List<DPJoin> dpJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public LockFreeParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        Map<Expression, KnaryBoolEval> predToComp = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        // Initialize multi-way join operator
        int nrTables = query.nrJoined;
        int nrSplits = query.equiJoinPreds.size() + nrTables;
        ParallelProgressTracker tracker = new ParallelProgressTracker(nrTables, nrThreads, nrSplits);
        for (int i = 0; i < nrThreads; i++) {
            ModJoin modJoin = new ModJoin(query, context, budget, nrThreads, i, predToEval, predToComp);
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
        // initialize an end plan.
        EndPlan endPlan = new EndPlan(nrThreads, nrTables, dpJoins.get(0).cardinalities);
        List<LockFreeTask> tasks = new ArrayList<>();
        // Mutex shared by multiple threads.
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean end = new AtomicBoolean(false);
        AtomicBoolean finish = new AtomicBoolean(false);
        // logs list
        List<String>[] logs = new List[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            logs[i] = new ArrayList<>();
        }
        for (int i = 0; i < nrThreads; i++) {
            LockFreeTask lockFreeTask = new LockFreeTask(query, context, root, endPlan, end, finish, lock, dpJoins.get(i));
            tasks.add(lockFreeTask);
        }
        long executionStart = System.currentTimeMillis();
        List<Future<LockFreeResult>> futures = executorService.invokeAll(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        JoinStats.subExeTime.add(JoinStats.exeTime);
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
            LogUtils.writeLogs(logs, "verbose/lockFree/" + QueryStats.queryName);
        }

        System.out.println("Result Set: " + resultList.size());
    }
}
